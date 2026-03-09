package main

import (
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net/url"
	"os"
	"os/user"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"golang.org/x/text/message"

	"github.com/charmbracelet/bubbles/table"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

var (
	baseStyle = lipgloss.NewStyle().
			BorderStyle(lipgloss.NormalBorder()).
			BorderForeground(lipgloss.Color("240"))

	listStyle = lipgloss.NewStyle().
			Border(lipgloss.NormalBorder()).
			BorderForeground(lipgloss.Color("240")).
			Padding(1, 2).
			Width(20)

	selectedItemStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("229")).
				Background(lipgloss.Color("57"))

	itemStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("245"))

	actions = []string{
		"Open",
		"Rename",
		"Delete",
	}
)

type LibraryItem struct {
	Title         string
	TotalSize     int64
	GUID          string
	CreatedAt     float64
	MetadataID    int
	ElderGUID     string
	NumberStreams int
	LastWatched   float64
	Seasons       []LibraryItemSeason
	Bitrate       float64
}

type LibraryItemSeason struct {
	Title          string
	TotalSize      int64
	MetadataID     int
	ParentID       int
	NumberChildren int
	CreatedAt      float64
	LastWatched    float64
	AvgBitrate     float64
}

type Page int64

const (
	LibraryPicker Page = iota
	LibraryView
)

type LocalConfig struct {
	FilterCreatedBeforeMonths int    `json:"filter_created_before_months" default:"18"`
	FilterLastStreamedMonths  int    `json:"filter_last_streamed_months" default:"18"`
	PlexDBPath                string `json:"plex_db_path" default:""`
	Language                  string `json:"language" default:"en"`
}

type LibrarySummary struct {
	ID     int
	Name   string
	SizeGB float64
}

type StaleResult struct {
	LibraryID     int
	LibraryName   string
	MetadataID    int
	Title         string
	SizeGB        float64
	BitrateMbps   float64
	Created       string
	LastStreamed  string
}

type Model struct {
	page     Page
	selected int // which to-do items are selected
	err      error
	table    table.Model
	db       *sql.DB
	printer  *message.Printer

	libraryID            int
	maxLibraryNameLength float64
}

var CONFIG LocalConfig
var VERBOSE bool

func sqliteReadOnlyDSN(dbPath string) string {
	dsn := &url.URL{Scheme: "file", Path: dbPath}
	query := dsn.Query()
	query.Set("mode", "ro")
	query.Set("immutable", "1")
	query.Set("_query_only", "1")
	dsn.RawQuery = query.Encode()
	return dsn.String()
}

func main() {
	VERBOSE = false
	CONFIG = LocalConfig{}
	var configPath string
	var outputPath string

	if len(os.Args) > 1 {
		for _, arg := range os.Args[1:] {
			if strings.HasPrefix(arg, "--config=") {
				configPath = strings.TrimPrefix(arg, "--config=")
			} else if strings.HasPrefix(arg, "--output=") {
				outputPath = strings.TrimPrefix(arg, "--output=")
			} else {
				fmt.Println("Error: Unrecognized argument: ", arg)
				fmt.Println("Usage: staleaudit-for-plex [--config=<path to config file>] [--output=<path to csv file>]")
				os.Exit(1)
			}
		}
	}
	loadConfig(configPath)

	MODEL := Model{table: table.Model{}, selected: 0, err: nil}

	// this query seems to work, but returns less rows than expected
	// seems to stop in 2022
	// select strftime('%Y-%m-%d', mss.created_at, 'unixepoch'), name, title from media_streams ms inner join  media_stream_settings mss on ms.id = mss.media_stream_id inner join  accounts a on a.id = mss.account_id inner join media_items mi on mi.id = ms.media_item_id inner join metadata_items metaitems on metaitems.id = mi.metadata_item_id order by ms.created_at asc;

	// this is the views
	// select strftime('%Y-%m-%d', miv.viewed_at, 'unixepoch'), name, miv.guid, mi.title from metadata_item_views as miv inner join accounts a on a.id = miv.account_id inner join metadata_items mi on mi.guid = miv.guid order by miv.viewed_at asc

	// the items are in metadata_items

	// Connect to the Plex sqlite server

	db, err := sql.Open("sqlite3", sqliteReadOnlyDSN(CONFIG.PlexDBPath))
	if err != nil {
		log.Fatal(err)
	}
	if err := db.Ping(); err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	MODEL.db = db
	MODEL.printer = message.NewPrinter(message.MatchLanguage(CONFIG.Language))

	if outputPath != "" {
		if err := MODEL.exportAllLibrariesToCSV(outputPath); err != nil {
			log.Fatal(err)
		}
		fmt.Println("Wrote CSV to " + outputPath)
		return
	}

	MODEL.prepareLibraryPickerPage()
	if _, err := tea.NewProgram(MODEL).Run(); err != nil {
		fmt.Println("Error running program:", err)
		os.Exit(1)
	}

}

func loadConfig(configLocationInput string) {
	var configLocation string
	var plexRoot string
	slash := string(os.PathSeparator)
	opsys := runtime.GOOS
	if VERBOSE {
		fmt.Println("OS: " + opsys)
	}
	if strings.HasPrefix("windows/", opsys) {
		dir, _ := os.UserCacheDir()
		plexRoot = dir + slash + "Plex Media Server"
	} else if strings.HasPrefix("darwin/", opsys) {
		usr, _ := user.Current()
		plexRoot = usr.HomeDir + "/Library/Application Support/Plex Media Server"
	} else if strings.HasPrefix("linux/", opsys) {
		plexRoot = "$PLEX_HOME/Library/Application Support/Plex Media Server"
	} else {
		fmt.Println("Error: Unrecognized OS prefix: ", opsys)
		os.Exit(1)
	}

	if configLocationInput == "" {
		configLocation = os.ExpandEnv(plexRoot + slash + "staleaudit-for-plex.json")
	} else {
		configLocation = os.ExpandEnv(configLocationInput)
		if !strings.Contains(configLocationInput, slash) {
			configLocation = "." + slash + configLocationInput
		}
	}
	configDir := strings.TrimSuffix(configLocation, slash+filepath.Base(configLocation))
	if _, err := os.Stat(configDir); os.IsNotExist(err) {
		fmt.Println("Error: Directory does not exist:", configDir)
		os.Exit(1)
	}

	if _, err := os.Stat(configLocation); os.IsNotExist(err) {
		fmt.Println("Creating a new config...")
	} else {
		if VERBOSE {
			fmt.Println("Using an existing config...")
		}
		// Load the existing file.
		configFh, err := os.Open(configLocation)
		if err != nil {
			log.Fatalf("Error opening config %s: %v", configLocation, err)
		}
		decoder := json.NewDecoder(configFh)
		if err := decoder.Decode(&CONFIG); err != nil && !errors.Is(err, io.EOF) {
			_ = configFh.Close()
			log.Fatalf("Error decoding config %s: %v", configLocation, err)
		}
		if err := configFh.Close(); err != nil {
			log.Fatalf("Error closing config %s: %v", configLocation, err)
		}
	}

	if CONFIG.PlexDBPath == "" {
		CONFIG.PlexDBPath = os.ExpandEnv(plexRoot + slash + "Plug-in Support" + slash + "Databases" + slash + "com.plexapp.plugins.library.db")
	}
	if CONFIG.Language == "" {
		CONFIG.Language = "en"
	}
	if CONFIG.FilterCreatedBeforeMonths == 0 {
		CONFIG.FilterCreatedBeforeMonths = 18
	}
	if CONFIG.FilterLastStreamedMonths == 0 {
		CONFIG.FilterLastStreamedMonths = 18
	}

	configFh, err := os.OpenFile(configLocation, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		log.Fatalf("Error opening config %s for writing: %v", configLocation, err)
	}
	defer configFh.Close()

	encoder := json.NewEncoder(configFh)
	if err := encoder.Encode(&CONFIG); err != nil {
		log.Fatalf("Error encoding config %s: %v", configLocation, err)
	}
	if opsys != "windows" {
		if err := os.Chmod(configLocation, 0o600); err != nil {
			log.Fatalf("Error setting config permissions on %s: %v", configLocation, err)
		}
	}

	if VERBOSE {
		fmt.Println("Using config at " + configLocation)
		fmt.Println("Using Plex DB at " + CONFIG.PlexDBPath)
		fmt.Println("Using language " + CONFIG.Language)
	}
}

func (m *Model) listLibraries() ([]LibrarySummary, error) {
	libraries := []LibrarySummary{}
	rows, err := m.db.Query("SELECT library_section_id, name, sum(size) as s FROM media_items INNER JOIN library_sections ls ON ls.id = library_section_id WHERE deleted_at IS NULL AND library_section_id > 0 GROUP BY library_section_id ORDER BY s DESC")
	if err != nil {
		return nil, fmt.Errorf("media items query: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var library_section_id int
		var name string
		var size float64
		err := rows.Scan(&library_section_id, &name, &size)
		if err != nil {
			return nil, err
		}
		libraries = append(libraries, LibrarySummary{
			ID:     library_section_id,
			Name:   name,
			SizeGB: size / 1000.00 / 1000.00 / 1000.00,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return libraries, nil
}

func (m *Model) prepareLibraryPickerPage() {
	tableRows := []table.Row{}

	libraries, err := m.listLibraries()
	if err != nil {
		log.Fatalln("DB location: " + CONFIG.PlexDBPath)
		log.Fatal(err)
	}
	m.maxLibraryNameLength = 0
	for _, library := range libraries {
		m.maxLibraryNameLength = math.Max(m.maxLibraryNameLength, float64(len(library.Name)))
		tableRows = append(tableRows, table.Row{
			fmt.Sprintf("%d", library.ID),
			library.Name,
			m.printer.Sprintf("%.2f", library.SizeGB),
		})
	}

	tableColumns := []table.Column{
		{Title: "ID", Width: 4},
		{Title: "Library Name", Width: int(math.Max(15, math.Min(36, m.maxLibraryNameLength)))},
		{Title: "Size in Gb", Width: 15},
	}

	t := table.New(
		table.WithColumns(tableColumns),
		table.WithRows(tableRows),
		table.WithFocused(true),
		table.WithHeight(int(math.Max(10, math.Min(30, float64((len(tableRows))))))), // min 10 or (max (30 or number of rows))
	)

	s := table.DefaultStyles()
	s.Header = s.Header.
		BorderStyle(lipgloss.NormalBorder()).
		BorderForeground(lipgloss.Color("240")).
		BorderBottom(true).
		Bold(false)
	s.Selected = s.Selected.
		Foreground(lipgloss.Color("229")).
		Background(lipgloss.Color("57")).
		Bold(false)
	t.SetStyles(s)

	m.table = t

}

func (m *Model) collectStaleResults(library LibrarySummary) ([]StaleResult, error) {
	query := "SELECT guid, metadata_items.id, title, metadata_items.created_at, coalesce(size,0), coalesce(bitrate, 0) FROM metadata_items LEFT JOIN media_items on media_items.metadata_item_id = metadata_items.id WHERE metadata_items.guid not like 'collection://%' AND parent_id is null and metadata_items.library_section_id = ?;"
	rows, err := m.db.Query(query, library.ID)
	if err != nil {
		return nil, fmt.Errorf("metadata items query: %w", err)
	}
	libraryItems := make(map[string]LibraryItem)
	idToGuidMap := make(map[int]string)
	for rows.Next() {
		var id int
		var title string
		var guid string
		var createdAt float64
		var size int64
		var bitrate float64
		err := rows.Scan(&guid, &id, &title, &createdAt, &size, &bitrate)
		if err != nil {
			rows.Close()
			return nil, err
		}
		if libraryItems[guid].MetadataID != 0 {
			item := libraryItems[guid]
			item.TotalSize += size
			item.CreatedAt = math.Min(item.CreatedAt, createdAt)
		} else {
			libraryItems[guid] = LibraryItem{Title: title, TotalSize: size, MetadataID: id, NumberStreams: 0, LastWatched: 0, CreatedAt: createdAt, Bitrate: bitrate}
		}

		idToGuidMap[id] = guid
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}

	allSeasons := make(map[int]LibraryItemSeason)
	query = "select season.parent_id, season.id, season.title, sum(size) as size, count(1) as count, avg(bitrate) as avgbitrate FROM media_items INNER JOIN metadata_items episode ON  media_items.metadata_item_id = episode.id INNER JOIN metadata_items season ON season.id = episode.parent_id WHERE episode.library_section_id = ? GROUP BY season.id;"
	rows, err = m.db.Query(query, library.ID)
	if err != nil {
		return nil, fmt.Errorf("children size query: %w", err)
	}
	for rows.Next() {
		var parentID int
		var seasonID int
		var title string
		var size int64
		var count int
		var avgBitrate float64
		err := rows.Scan(&parentID, &seasonID, &title, &size, &count, &avgBitrate)
		if err != nil {
			rows.Close()
			return nil, fmt.Errorf("children size counting: %w", err)
		}
		allSeasons[seasonID] = LibraryItemSeason{Title: title, TotalSize: size, MetadataID: seasonID, ParentID: parentID, NumberChildren: count, AvgBitrate: avgBitrate}
		parentGUID := idToGuidMap[parentID]
		l := libraryItems[parentGUID]
		l.TotalSize += size
		l.Seasons = append(l.Seasons, allSeasons[seasonID])
		l.Bitrate = avgBitrate
		libraryItems[parentGUID] = l
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}

	query = "SELECT grandparent_guid, coalesce(size, 0), miv.guid, coalesce(parent_id, 0), miv.viewed_at FROM metadata_item_views as miv INNER JOIN metadata_items mi ON mi.guid = miv.guid LEFT JOIN media_items on media_items.metadata_item_id = mi.id WHERE grandparent_guid is not null and mi.library_section_id = ? and miv.viewed_at > 0 ORDER BY miv.viewed_at ASC;"
	rows, err = m.db.Query(query, library.ID)
	if err != nil {
		return nil, fmt.Errorf("views query: %w", err)
	}
	for rows.Next() {
		var guid string
		var parentID int
		var size int64
		var grandparentGuid string
		var viewedAt float64
		err := rows.Scan(&grandparentGuid, &size, &guid, &parentID, &viewedAt)
		if err != nil {
			rows.Close()
			return nil, err
		}

		if grandparentGuid != "" {
			guid = grandparentGuid
		}

		item := libraryItems[guid]
		//fmt.Println("Upading item", item.Title, item.GUID, item.NumberStreams, viewedAt)
		item.NumberStreams++
		item.LastWatched = math.Max(viewedAt, item.LastWatched)
		libraryItems[guid] = item
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}

	results := make([]StaleResult, 0)
	createdLongAgo := float64(time.Now().AddDate(0, -1*CONFIG.FilterCreatedBeforeMonths, 0).Unix())
	lastStreamedLongAgo := float64(time.Now().AddDate(0, -1*CONFIG.FilterLastStreamedMonths, 0).Unix())
	for _, item := range libraryItems {
		if item.CreatedAt > createdLongAgo {
			continue
		}

		if item.LastWatched < lastStreamedLongAgo {
			lastWatchedStr := "never"
			if item.LastWatched > 1000000 {
				lastWatchedStr = time.Unix(int64(item.LastWatched), 0).Format("2006-01-02")
			}
			results = append(results, StaleResult{
				LibraryID:    library.ID,
				LibraryName:  library.Name,
				MetadataID:   item.MetadataID,
				Title:        item.Title,
				SizeGB:       float64(item.TotalSize) / 1000.00 / 1000.00 / 1000.00,
				BitrateMbps:  item.Bitrate / 1000.0 / 1000.0,
				Created:      time.Unix(int64(item.CreatedAt), 0).Format("2006-01-02"),
				LastStreamed: lastWatchedStr,
			})
		}
	}
	sort.Slice(results, func(i, j int) bool {
		return results[i].SizeGB > results[j].SizeGB
	})
	return results, nil
}

func (m *Model) prepareLibraryViewPage() {
	results, err := m.collectStaleResults(LibrarySummary{ID: m.libraryID})
	if err != nil {
		log.Fatal(err)
	}

	decayingRows := make([]table.Row, 0, len(results))
	maxTitleLength := 25.0
	for _, result := range results {
		maxTitleLength = math.Max(maxTitleLength, float64(len(result.Title)))
		decayingRows = append(decayingRows, table.Row{
			fmt.Sprintf("%d", result.MetadataID),
			result.Title,
			m.printer.Sprintf("%.2f", result.SizeGB),
			m.printer.Sprintf("%.1f", result.BitrateMbps),
			result.Created,
			result.LastStreamed,
		})
	}
	tableColumns := []table.Column{
		{Title: "ID", Width: 10},
		{Title: "Name", Width: int(math.Max(25, math.Min(50, maxTitleLength)))},
		{Title: "Size (Gb)", Width: 15},
		{Title: "Bitrate (Mb/s)", Width: 15},
		{Title: "Created", Width: 12},
		{Title: "Last Streamed", Width: 15},
	}

	decayingTable := table.New(
		table.WithColumns(tableColumns),
		table.WithRows(decayingRows),
		table.WithFocused(true),
		table.WithHeight(int(math.Max(10, math.Min(20, float64((len(decayingRows))))))), // min 10 or (max (30 or number of rows))
	)

	s := table.DefaultStyles()
	s.Header = s.Header.
		BorderStyle(lipgloss.NormalBorder()).
		BorderForeground(lipgloss.Color("240")).
		BorderBottom(true).
		Bold(false)
	s.Selected = s.Selected.
		Foreground(lipgloss.Color("229")).
		Background(lipgloss.Color("57")).
		Bold(false)
	decayingTable.SetStyles(s)

	m.table = decayingTable
}

func (m *Model) exportAllLibrariesToCSV(outputPath string) error {
	libraries, err := m.listLibraries()
	if err != nil {
		return err
	}

	outputDir := filepath.Dir(outputPath)
	if _, err := os.Stat(outputDir); os.IsNotExist(err) {
		return fmt.Errorf("output directory does not exist: %s", outputDir)
	}

	outputFile, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("error opening output %s: %w", outputPath, err)
	}
	defer outputFile.Close()

	writer := csv.NewWriter(outputFile)
	defer writer.Flush()

	if err := writer.Write([]string{"library_id", "library_name", "metadata_id", "title", "size_gb", "bitrate_mbps", "created", "last_streamed"}); err != nil {
		return err
	}

	for _, library := range libraries {
		results, err := m.collectStaleResults(library)
		if err != nil {
			return err
		}
		for _, result := range results {
			if err := writer.Write([]string{
				strconv.Itoa(result.LibraryID),
				result.LibraryName,
				strconv.Itoa(result.MetadataID),
				result.Title,
				fmt.Sprintf("%.2f", result.SizeGB),
				fmt.Sprintf("%.1f", result.BitrateMbps),
				result.Created,
				result.LastStreamed,
			}); err != nil {
				return err
			}
		}
	}

	if err := writer.Error(); err != nil {
		return err
	}
	return nil
}

// how to: api call for optimize media.
// host, session, token, and Client-Identifier redacted
// curl 'https://x.plex.direct:32400/playlists/88/items?Item%5Btype%5D=42&Item%5Btitle%5D=Jojo%20Rabbit&Item%5Btarget%5D=Custom%3A%20Universal%20TV&Item%5BtargetTagID%5D=&Item%5BlocationID%5D=-1&Item%5BLocation%5D%5Buri%5D=library%3A%2F%2Fd7a0632c-2227-401b-bea8-f19adeb9c1f9%2Fitem%2F%252Flibrary%252Fmetadata%252F8121&Item%5BDevice%5D%5Bprofile%5D=Universal%20TV&Item%5BPolicy%5D%5Bscope%5D=all&Item%5BPolicy%5D%5Bvalue%5D=&Item%5BPolicy%5D%5Bunwatched%5D=0&Item%5BMediaSettings%5D%5BvideoQuality%5D=60&Item%5BMediaSettings%5D%5BvideoResolution%5D=1920x1080&Item%5BMediaSettings%5D%5BmaxVideoBitrate%5D=8000&Item%5BMediaSettings%5D%5BaudioBoost%5D=&Item%5BMediaSettings%5D%5BsubtitleSize%5D=&Item%5BMediaSettings%5D%5BmusicBitrate%5D=&Item%5BMediaSettings%5D%5BphotoQuality%5D=&Item%5BMediaSettings%5D%5BphotoResolution%5D=&X-Plex-Product=Plex%20Web&X-Plex-Version=4.145.1&X-Plex-Client-Identifier=x&X-Plex-Platform=Firefox&X-Plex-Platform-Version=137.0&X-Plex-Features=external-media%2Cindirect-media%2Chub-style-list&X-Plex-Model=standalone&X-Plex-Device=OSX&X-Plex-Device-Name=Firefox&X-Plex-Device-Screen-Resolution=1388x763%2C1440x900&X-Plex-Token=&X-Plex-Language=en&X-Plex-Session-Id=' --compressed -X PUT -H 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:137.0) Gecko/20100101 Firefox/137.0' -H 'Accept: text/plain, */*; q=0.01' -H 'Accept-Language: en' -H 'Accept-Encoding: gzip, deflate, br, zstd' -H 'Origin: https://app.plex.tv' -H 'Sec-GPC: 1' -H 'Connection: keep-alive' -H 'Referer: https://app.plex.tv/' -H 'Sec-Fetch-Dest: empty' -H 'Sec-Fetch-Mode: cors' -H 'Sec-Fetch-Site: cross-site' -H 'DNT: 1' -H 'Priority: u=0' -H 'Pragma: no-cache' -H 'Cache-Control: no-cache' -H 'Content-Length: 0'

func (m Model) Init() tea.Cmd {
	return nil
}

func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "esc":
			if m.table.Focused() {
				m.table.Blur()
			} else {
				m.table.Focus()
			}
		case "q", "ctrl+c":
			return m, tea.Quit
		case "enter":
			if m.page == LibraryPicker {
				m.libraryID, _ = strconv.Atoi(m.table.SelectedRow()[0])
				m.prepareLibraryViewPage()
				m.page++
			}
			return m, tea.Batch()
		}
	}
	m.table, cmd = m.table.Update(msg)
	return m, cmd
}

func (m Model) View() string {
	//fmt.Println("M.page is ", m.page)
	switch m.page {
	case LibraryPicker:
		return baseStyle.Render(m.table.View()) + "\n"
	case LibraryView:
		return lipgloss.JoinHorizontal(
			lipgloss.Top,
			lipgloss.JoinVertical(lipgloss.Top,
				"Displaying items added more than "+strconv.Itoa(CONFIG.FilterCreatedBeforeMonths)+" months ago, and not streamed in the last "+strconv.Itoa(CONFIG.FilterLastStreamedMonths)+" months.\n",
				baseStyle.Render(m.table.View())),
		) + "\n"
	}
	return "Error - unrecognized page"
}
