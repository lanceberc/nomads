package main

import "os"
import "os/user"
import "os/exec"
import "syscall"
import "sync"
import "time"
import "io"
import "fmt"
import "flag"
import "log"
import "sort"

type Longitude struct{ west, east float64 }
type Latitude struct{ north, south float64 }

type Zone struct {
	description string
	geo         string
	model       string
	longitude   Longitude
	latitude    Latitude
	variables   string
}

var zones = map[string]Zone{
	"sf": Zone{
		description: "SF Bay Wind hi-res (18 hour hrrr)",
		geo:         "sf",
		model:       "hrrr",
		longitude:   Longitude{-123.0, -121.0},
		latitude:    Latitude{38.5, 36.0},
		variables:   "hrrr-base",
	},
	"socal": Zone{
		description: "SoCal Bay Wind hi-res (18 hour hrrr)",
		geo:         "socal",
		model:       "hrrr",
		longitude:   Longitude{-120.5, -116.5},
		latitude:    Latitude{34.5, 32.0},
		variables:   "hrrr-wind",
	},
	"sf36": Zone{
		description: "SF Bay Wind hi-res (36 hour hrrr, runs every 6 hours)",
		geo:         "sf",
		model:       "hrrr36",
		longitude:   Longitude{-123, -122},
		latitude:    Latitude{38, 37},
		variables:   "hrrr-wind",
	},
	"sfoffshore": Zone{
		description: "SF Bay & Farallones Wind hi-res (18 hour hrrr)",
		geo:         "hrrr",
		model:       "sfoffshore",
		longitude:   Longitude{-131, -119},
		latitude:    Latitude{41, 35},
		variables:   "hrrr-base",
	},
	"sfoffshore36": Zone{
		description: "SF Bay & Farallones Wind hi-res (36 hour hrrr)",
		geo:         "sfoffshore",
		model:       "hrrr36",
		longitude:   Longitude{-131, -119},
		latitude:    Latitude{41, 35},
		variables:   "hrrr-wind",
	},
	"sfsub": Zone{
		description: "SF Bay Wind hi-res, 15m intervals (18 hour hrrr_sub)",
		geo:         "sf",
		model:       "hrrr_sub",
		longitude:   Longitude{-123, -122},
		latitude:    Latitude{38, 37},
		variables:   "hrrr-wind",
	},
	"norcal": Zone{
		description: "Bay Area (incl Monterey Bay) all variables (18 hour hrrr)",
		geo:         "norcal",
		model:       "hrrr",
		longitude:   Longitude{-123, -121},
		latitude:    Latitude{38, 36},
		variables:   "hrrr-all",
	},
	"sf96": Zone{
		description: "SF Bay Wind (96 hour GFS)",
		geo:         "sf96",
		model:       "gfs_hourly",
		longitude:   Longitude{-124.5, -122},
		latitude:    Latitude{38.5, 36.5},
		variables:   "gfs-wind",
	},
	"sfnam": Zone{
		description: "Outside SF Bay (60 hour NAM)",
		geo:         "sfnam",
		model:       "nam-nest",
		longitude:   Longitude{-124.5, -122},
		latitude:    Latitude{38.5, 36.5},
		variables:   "nam-base",
	},
	"socalnam": Zone{
		description: "SoCal Wind (60 hour NAM)",
		geo:         "socalnam",
		model:       "nam-nest",
		longitude:   Longitude{-120.5, -116.5},
		latitude:    Latitude{34.5, 32.0},
		variables:   "nam-wind",
	},
	"casnownam": Zone{
		description: "California Coast and Mountains (60 hour NAM)",
		geo:         "ca",
		model:       "nam-nest",
		longitude:   Longitude{-137.0, -117.0},
		latitude:    Latitude{43.0, 32.0},
		variables:   "nam-winter",
	},
	"canam": Zone{
		description: "California Coast (60 hour NAM)",
		geo:         "ca",
		model:       "nam-nest",
		longitude:   Longitude{-130.0, -116.0},
		latitude:    Latitude{42.0, 32.5},
		variables:   "nam-wind",
	},
	"canamall": Zone{
		description: "California Coast (60 hour NAM)",
		geo:         "ca",
		model:       "nam-nest24",
		longitude:   Longitude{-130.0, -116.0},
		latitude:    Latitude{42.0, 32.5},
		variables:   "nam-all",
	},
	"cahrrr": Zone{
		description: "California Coast Wind (18 hour HRRR)",
		geo:         "ca",
		model:       "hrrr",
		longitude:   Longitude{-130.0, -116.0},
		latitude:    Latitude{42.0, 32.5},
		variables:   "hrrr-wind",
	},
	"caall": Zone{
		description: "California Coast Wind (18 hour HRRR)",
		geo:         "ca",
		model:       "hrrr",
		longitude:   Longitude{-130.0, -116.0},
		latitude:    Latitude{42.0, 32.5},
		variables:   "hrrr-all",
	},
	"cahrrr36": Zone{
		description: "California Coast Wind (36 hour HRRR)",
		geo:         "ca",
		model:       "hrrr36",
		longitude:   Longitude{-130.0, -116.0},
		latitude:    Latitude{42.0, 32.5},
		variables:   "hrrr-wind",
	},
	"fire": Zone{
		description: "SF North Bay fire (18 hour hrrr_sub)",
		geo:         "fire",
		model:       "hrrr_sub",
		longitude:   Longitude{-123, -121},
		latitude:    Latitude{39, 37},
		variables:   "hrrr-wind",
	},
	"tahoe": Zone{
		description: "Tahoe area (18 hour hrrr)",
		geo:         "tahoe",
		model:       "hrrr",
		longitude:   Longitude{-121, -119},
		latitude:    Latitude{40, 38},
		variables:   "hrrr-base",
	},
	"tahoenam": Zone{
		description: "Tahoe area winter (60 hour NAM)",
		geo:         "tahoe",
		model:       "nam-nest",
		longitude:   Longitude{-123.0, -118.0},
		latitude:    Latitude{42.0, 36.0},
		variables:   "nam-winter",
	},
	"pacific": Zone{
		description: "North Pacific Wind/Precip (10 day GFS)",
		geo:         "pacific",
		model:       "gfs",
		longitude:   Longitude{-230, -100},
		latitude:    Latitude{70, 10},
		variables:   "gfs-base",
	},
	"s2h": Zone{
		description: "Sydney to Hobart (10 day GFS)",
		geo:         "s2h",
		model:       "gfs",
		longitude:   Longitude{138, 163},
		latitude:    Latitude{-30, -46},
		variables:   "gfs-base",
	},
	"paccup": Zone{
		description: "PacCup Wind (10 day 1-degree GFS)",
		geo:         "paccup",
		model:       "gfs_lores",
		longitude:   Longitude{-160, -115},
		latitude:    Latitude{50, 15},
		variables:   "gfs-wind",
	},
	"la": Zone{
		description: "Los Angeles Wind hi-res (18 hour hrrr)",
		geo:         "la",
		model:       "hrrr",
		longitude:   Longitude{-122.0, -117.0},
		latitude:    Latitude{36.0, 32.0},
		variables:   "hrrr-base",
	},
	"lanam": Zone{
		description: "Los Angeles NAM",
		geo:         "la",
		model:       "nam-nest",
		longitude:   Longitude{-122.0, -117.0},
		latitude:    Latitude{36.0, 32.0},
		variables:   "nam-wind",
	},
	"se": Zone{
		description: "IOD Sweden Race Ares",
		geo:         "stenungsund",
		model:       "gfs",
		longitude:   Longitude{0, 20},
		latitude:    Latitude{65, 50},
		variables:   "gfs-wind",
	},
	"volvo": Zone{
		description: "Wherever the Volvo Ocean Race is",
		geo:         "volvo",
		model:       "gfs",
		longitude:   Longitude{140, 170},
		latitude:    Latitude{0, -40},
		variables:   "gfs-base",
	},
	"utah": Zone{
		description: "Big Sky to Wasatch",
		geo:         "utah",
		model:       "hrrr",
		longitude:   Longitude{-118, -105},
		latitude:    Latitude{48, 38},
		variables:   "hrrr-base",
	},
	"colorado": Zone{
		description: "Coloradoh",
		geo:         "colorado",
		model:       "hrrr",
		longitude:   Longitude{-109, -102},
		latitude:    Latitude{41, 37},
		variables:   "hrrr-base",
	},
	"chessy": Zone{
		description: "Annapolis Wind hi-res (18 hour hrrr)",
		geo:         "chesapeake",
		model:       "hrrr",
		longitude:   Longitude{-77.0, -75.5},
		latitude:    Latitude{39.75, 38.5},
		variables:   "hrrr-base",
	},
	"newport": Zone{
		description: "Newport Wind hi-res (18 hour hrrr)",
		geo:         "newport",
		model:       "hrrr",
		longitude:   Longitude{-71.5, -71.0},
		latitude:    Latitude{41.75, 41.25},
		variables:   "hrrr-base",
	},
	"hamptons": Zone{
		description: "Hamptons to Newport Wind hi-res (18 hour hrrr)",
		geo:         "hamptons",
		model:       "hrrr",
		longitude:   Longitude{-72.5, -71.0},
		latitude:    Latitude{42.00, 40.00},
		variables:   "hrrr-base",
	},
	"hamptonssub": Zone{
		description: "Hamptons to Newport Wind hi-res (18 hour hrrr subhourly)",
		geo:         "hamptons",
		model:       "hrrr_sub",
		longitude:   Longitude{-72.5, -71.0},
		latitude:    Latitude{42.00, 40.00},
		variables:   "hrrr-wind",
	},
	"hamptonsnam": Zone{
		description: "Hamptons to Newport NAM",
		geo:         "hamptons",
		model:       "nam-nest",
		longitude:   Longitude{-72.5, -70.0},
		latitude:    Latitude{42.50, 40.00},
		variables:   "nam-base",
	},
	"hamptonsgfs": Zone{
		description: "New England GFS (10 day GFS)",
		geo:         "hamptons",
		model:       "gfs",
		longitude:   Longitude{-90, -55},
		latitude:    Latitude{50, 34},
		variables:   "gfs-base",
	},
	"dorian": Zone{
		description: "Dorian Wind hi-res (36 hour hrrr, runs every 6 hours)",
		geo:         "dorian",
		model:       "hrrr",
		longitude:   Longitude{-81, -76},
		latitude:    Latitude{29, 25},
		variables:   "gfs-base",
	},
	"doriannam": Zone{
		description: "Dorian NAM (60 hour NAM)",
		geo:         "dorian",
		model:       "nam-nest",
		longitude:   Longitude{-81, -76},
		latitude:    Latitude{29, 25},
		variables:   "nam-base",
	},
}

type Model struct {
	fn                string
	modelFrequency    string // Hours between model runs - assume all run at 00z
	forecastFrequency string // Hours between forecast steps
	horizon           string // Hours to last forecast in model run
	start             string // How long after the run starts the first forecast is usually available
	end               string // How long after the run starts the last forecast is usually avaialable
	baseurl           string // The URL with some fields to fill in
	baseurlfn         string // The filename associated with the forecast step URL
}

var models = map[string]Model{
	"gfs": {
		fn:                "gfs",  // filename for GRIB
		modelFrequency:    "6h",   // How often model runs (assume all models run at 00z)
		forecastFrequency: "6h",   // Time between forecasts
		horizon:           "384h", // When is last forecast?
		start:             "3.5h", // How long after run first forecast usually appears
		end:               "5h",   // How long after run last forecast usually appears
		baseurl:           "https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl?file=%s%s%s&subregion=&leftlon=%5.2f&rightlon=%5.2f&toplat=%5.2f&bottomlat=%5.2f&dir=%%2Fgfs.%04d%02d%02d%%2F%02d",
		baseurlfn:         "%s.t%02dz.pgrb2.0p25.f%03d",
	},
	"gfs_hourly": {
		fn:                "gfs",
		modelFrequency:    "6h",
		forecastFrequency: "1h",
		horizon:           "384h",
		start:             "3.5h", // gfs forecasts show up about 3 1/2  hours after model run
		end:               "5h",   // gfs 384 hour forecast completes about five hours after model run
		baseurl:           "https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25_1hr.pl?file=%s%s%s&subregion=&leftlon=%5.2f&rightlon=%5.2f&toplat=%5.2f&bottomlat=%5.2f&dir=%%2Fgfs.%04d%02d%02d%%2F%02d",
		baseurlfn:         "%s.t%02dz.pgrb2.0p25.f%03d",
	},
	"gfs_lores": {
		fn:                "gfs",  // filename for GRIB
		modelFrequency:    "6h",   // How often model runs (assume all models run at 00z)
		forecastFrequency: "6h",   // Time between forecasts
		horizon:           "384h", // When is last forecast?
		start:             "3.5h", // How long after run first forecast usually appears
		end:               "5h",   // How long after run last forecast usually appears
		baseurl:           "https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_1p00.pl?file=%s%s%s&subregion=&leftlon=%5.2f&rightlon=%5.2f&toplat=%5.2f&bottomlat=%5.2f&dir=%%2Fgfs.%04d%02d%02d%%2F%02d",
		baseurlfn:         "%s.t%02dz.pgrb2.1p00.f%03d",
	},
	"hrrr": {
		fn:                "hrrr",
		modelFrequency:    "1h",  // hrrr runs every hour
		forecastFrequency: "1h",  // forecasts are one hour apart
		horizon:           "18h", // hrrr is 18 hour forecast; for the 4 times a day it's longer use hrrr36
		start:             "50m", // hrrr f00 50 minutes after the hour
		end:               "85m", // f18 a bit more than 1/2 hour later
		baseurl:           "https://nomads.ncep.noaa.gov/cgi-bin/filter_hrrr_2d.pl?file=%s%s%s&subregion=&leftlon=%5.2f&rightlon=%5.2f&toplat=%5.2f&bottomlat=%5.2f&dir=%%2Fhrrr.%04d%02d%02d%%2Fconus",
		baseurlfn:         "%s.t%02dz.wrfsfcf%02d.grib2",
	},
	"hrrr36": {
		fn:                "hrrr",
		modelFrequency:    "6h",   // hrrr runs every hour, but every six hours the forecast is extended to 36 hours
		forecastFrequency: "1h",   // forecasts are one hour apart
		horizon:           "36h",  // how many hours of forecast to fetch
		start:             "50m",  // hrrr f00 50 minutes after the hour
		end:               "110m", // f36 usually an hour after f00
		baseurl:           "https://nomads.ncep.noaa.gov/cgi-bin/filter_hrrr_2d.pl?file=%s%s%s&subregion=&leftlon=%5.2f&rightlon=%5.2f&toplat=%5.2f&bottomlat=%5.2f&dir=%%2Fhrrr.%04d%02d%02d%%2Fconus",
		baseurlfn:         "%s.t%02dz.wrfsfcf%02d.grib2",
	},
	"hrrr_sub": { // Same as hrrr but has 15 minute sub-hourly forecasts
		fn:                "hrrr",
		modelFrequency:    "1h",
		forecastFrequency: "1h",
		horizon:           "18h",
		start:             "55m", // hrrr_sub f00 55 minutes after the hour
		end:               "85m", // f18 usually 25 - 30 minutes later
		baseurl:           "https://nomads.ncep.noaa.gov/cgi-bin/filter_hrrr_sub.pl?file=%s%s%s&subregion=&leftlon=%5.2f&rightlon=%5.2f&toplat=%5.2f&bottomlat=%5.2f&dir=%%2Fhrrr.%04d%02d%02d%%2Fconus",
		baseurlfn:         "%s.t%02dz.wrfsubhf%02d.grib2",
	},
	"nam": {
		fn:                "nam",
		modelFrequency:    "6h",   // hours between model runs - assume all run at 00z
		forecastFrequency: "1h",   // hours between forecast steps
		horizon:           "60h",  // NAM goes out 60 hours
		start:             "1.5h", // NAM forecasts show up about 1 1/2 hours after model run
		end:               "3h",   // NAM 60 hour forecast completes about three hours after model run
		baseurl:           "https://nomads.ncep.noaa.gov/cgi-bin/filter_nam.pl?file=%s%s%s&subregion=&leftlon=%5.2f&rightlon=%5.2f&toplat=%5.2f&bottomlat=%5.2f&dir=%%2Fnam.%04d%02d%02d",
		baseurlfn:         "%s.t%02dz.awphys%02d.tm00.grib2",
	},
	"nam-nest": {
		fn:                "nam",
		modelFrequency:    "6h",
		forecastFrequency: "1h",
		horizon:           "60h",
		start:             "1.5h",
		end:               "3h",
		baseurl:           "https://nomads.ncep.noaa.gov/cgi-bin/filter_nam_conusnest.pl?file=%s%s%s&subregion=&leftlon=%5.2f&rightlon=%5.2f&toplat=%5.2f&bottomlat=%5.2f&dir=%%2Fnam.%04d%02d%02d",
		baseurlfn:         "%s.t%02dz.conusnest.hiresf%02d.tm00.grib2",
	},
	"nam-nest24": {
		fn:                "nam",
		modelFrequency:    "6h",
		forecastFrequency: "2h",
		horizon:           "24h",
		start:             "1.5h",
		end:               "3h",
		baseurl:           "https://nomads.ncep.noaa.gov/cgi-bin/filter_nam_conusnest.pl?file=%s%s%s&subregion=&leftlon=%5.2f&rightlon=%5.2f&toplat=%5.2f&bottomlat=%5.2f&dir=%%2Fnam.%04d%02d%02d",
		baseurlfn:         "%s.t%02dz.conusnest.hiresf%02d.tm00.grib2",
	},
}

type VariableSet struct {
	models []string
	levels []string
	variables []string
}

var variableSets = map[string]VariableSet{
	"gfs-base": VariableSet{
		models: []string{"gfs", "gfs_hourly", "gfs_lores"},
		levels: []string{"mean_sea_level", "surface", "2_m_above_ground", "10_m_above_ground", "300_mb", "500_mb", "entire_atmosphere_%5C%28considered_as_a_single_layer%5C%29", "entire_atmosphere"},
		variables: []string{"PRMSL", "MSLET", "UGRD", "VGRD", "TMP", "APCP", "PWAT", "PRATE", "GUST", "HGT", "REFC", "CAPE"},
	},
	"gfs-wind": VariableSet{
		models: []string{"gfs", "gfs_hourly", "gfs_lores"},
		levels: []string{"mean_sea_level", "surface", "2_m_above_ground", "10_m_above_ground", "entire_atmosphere", "entire_atmosphere_%5C%28considered_as_a_single_layer%5C%29"},
		variables: []string{"APCP", "GUST", "PRATE", "PRES", "TMP", "UGRD", "VGRD", "CAPE"},
	},
	"nam-base": VariableSet{
		models: []string{"nam-nest"},
		levels: []string{"mean_sea_level", "surface", "2_m_above_ground", "10_m_above_ground", "entire_atmosphere_%5C%28considered_as_a_single_layer%5C%29"},
		variables: []string{"PRMSL", "UGRD", "VGRD", "TMP", "GUST", "MSLET", "REFC", "REFD", "MAXREF", "PWAT", "PRATE", "CAPE", "WEASD", "SRWEQ"},
	},
	"nam-winter": VariableSet{
		models: []string{"nam-nest"},
		levels: []string{"mean_sea_level", "surface", "2_m_above_ground", "10_m_above_ground", "entire_atmosphere_%5C%28considered_as_a_single_layer%5C%29"},
		variables: []string{"PRMSL", "UGRD", "VGRD", "TMP", "GUST", "MSLET", "REFC", "REFD", "MAXREF", "PWAT", "SNOD", "SNOHF", "SNOM", "SNOWC", "ICEC", "FRICV", "PRATE", "CAPE", "WEASD", "SRWEQ"},
	},
	"nam-wind": VariableSet{
		models: []string{"nam-nest"},
		levels: []string{"mean_sea_level", "surface", "2_m_above_ground", "10_m_above_ground"},
		variables: []string{"PRMSL", "UGRD", "VGRD", "TMP", "GUST", "CAPE"},
	},
	"nam-all": VariableSet{
		models: []string{"nam-nest24"},
		levels: []string{"all"},
		variables: []string{"all"},
	},
	"hrrr-base": VariableSet{
		models: []string{"hrrr", "hrrr36"},
		levels: []string{"mean_sea_level", "surface", "2_m_above_ground", "10_m_above_ground", "entire_atmosphere", "entire_atmosphere_%5C%28considered_as_a_single_layer%5C%29"},
		variables: []string{"APCP", "GUST", "PRATE", "PRES", "PWAT", "TMP", "UGRD", "VGRD", "WIND", "REFC", "REFD", "MAXREF", "CAPE", "LTNG"},
	},
	"hrrr-wind": VariableSet{
		models: []string{"hrrr", "hrrr36", "hrrr_sub"},
		levels: []string{"surface", "2_m_above_ground", "10_m_above_ground"},
		variables: []string{"PRES", "UGRD", "VGRD", "TMP", "WIND", "GUST", "REFC", "REFD"},
	},
	"hrrr-all": VariableSet{
		models: []string{"hrrr", "hrrr36", "hrrr_sub"},
		levels: []string{"all"},
		variables: []string{"all"},
	},
}

var help bool
var zone string
var prev bool
var partial bool
var merge bool
var refetch bool
var keep bool
var verbose bool
var threads int
var Z Zone
var M Model
var zulu time.Time

func prettyInt(i int64) string {
	const (
		Kilo int64 = 1024
		Mega       = 1024 * Kilo
		Giga       = 1024 * Mega
		Tera       = 1024 * Giga
		Peta       = 1024 * Tera
	)
	if i > Peta {
		return (fmt.Sprintf("%.2fPiB", float64(i)/float64(Peta)))
	}
	if i > Tera {
		return (fmt.Sprintf("%.2fTiB", float64(i)/float64(Tera)))
	}
	if i > Giga {
		return (fmt.Sprintf("%.2fGiB", float64(i)/float64(Giga)))
	}
	if i > Mega {
		return (fmt.Sprintf("%.2fMiB", float64(i)/float64(Mega)))
	}
	if i > Kilo {
		return (fmt.Sprintf("%.2fKiB", float64(i)/float64(Kilo)))
	}
	return (fmt.Sprintf("%4dB", i))

}

func fetchUrlWithCurl(url, fn string) (exitCode int) {
	if verbose {
		log.Printf("%s %s %s %s\n", "curl", "-o", fn, url)
	}
	cmd := exec.Command("curl", "--silent", "-o", fn, url)
	//	cmd := exec.Command("curl", "--silent", "--compress", "-o", fn, url)
	//	cmd := exec.Command("curl", "--silent", "-o", fn, url)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		// try to get the exit code
		if exitError, ok := err.(*exec.ExitError); ok {
			ws := exitError.Sys().(syscall.WaitStatus)
			exitCode = ws.ExitStatus()
		} else {
			// This will happen (in OSX) if `name` is not available in $PATH,
			// in this situation, exit code could not be get, and stderr will be
			// empty string very likely, so we use the default fail code, and format err
			// to string and set to stderr
			log.Printf("Could not get exit code for failed program: %v", "curl")
			exitCode = 1
		}
	} else {
		// success, exitCode should be 0
		ws := cmd.ProcessState.Sys().(syscall.WaitStatus)
		exitCode = ws.ExitStatus()
	}
	return (exitCode)
}

var mu sync.Mutex
var nextForecast = 0
var wg sync.WaitGroup

type result struct {
	forecast int
	result   string // "ok", "exists", "bad"
	filename string // outputfn
}

var forecasts []int
var results []result
var inProgress bool

func storeResult(i int, f int, r string, fn string) {
	mu.Lock()
	results[i].forecast = f
	results[i].result = r
	results[i].filename = fn
	mu.Unlock()
}

func fetchForecasts(id int, levels string, vars string, runDir string) {
	knownCurlErrors := map[int]string{
		7:  "connection timed out",
		18: "connection closed with data remaining",
		56: "connection reset",
	}

	for true {
		// Get next forecast to fetch
		mu.Lock()
		thisIndex := nextForecast
		nextForecast++
		mu.Unlock()
		if thisIndex >= len(forecasts) {
			if verbose {
				log.Printf("Thread %d done\n", id)
			}
			wg.Done()
			return
		}
		forecast := forecasts[thisIndex]

		urlfn := fmt.Sprintf(M.baseurlfn, M.fn, zulu.Hour(), forecast)
		if verbose {
			log.Printf("Fetching #%d: %d %s\n", thisIndex, forecast, urlfn)
		}

		// GFS has one directory per day, others one directory per model run
		var url string
		if (Z.model == "gfs") || (Z.model == "gfs_hourly") || (Z.model == "gfs_lores") {
			url = fmt.Sprintf(M.baseurl, urlfn, levels, vars, Z.longitude.west, Z.longitude.east, Z.latitude.north, Z.latitude.south, zulu.Year(), int(zulu.Month()), zulu.Day(), zulu.Hour())
		} else {
			url = fmt.Sprintf(M.baseurl, urlfn, levels, vars, Z.longitude.west, Z.longitude.east, Z.latitude.north, Z.latitude.south, zulu.Year(), int(zulu.Month()), zulu.Day())
		}

		fn := runDir + "/" + urlfn

		_, err := os.Stat(fn)
		if err == nil {
			log.Printf("Skip (exists) %s\n", urlfn)
			storeResult(thisIndex, forecast, "exists", fn)
			continue
		}

		log.Printf("Thread %d Fetching %s\n", id, urlfn)
		ok := true
		attempts := 0
		errCode := 1
		for (errCode != 0) && (attempts <= 5) && (ok) {
			attempts += 1
			if attempts > 1 {
				log.Printf("Curl attempt #%d %v\n", attempts, urlfn)
			}
			if errCode = fetchUrlWithCurl(url, fn); errCode != 0 {
				if verbose {
					log.Printf("fetchUrl returns %v\n", errCode)
				}
				fault, ok := knownCurlErrors[errCode]
				if ok {
					log.Printf("curl: failure %d: %s\n", errCode, fault)
				} else {
					log.Printf("curl: failure %d: %s\n", errCode, "Unexpected fault")
				}
			}
		}
		if !ok {
			_ = os.Remove(fn)
			storeResult(thisIndex, forecast, "bad", "")
			continue
		}

		// check to see if it's a GRIB
		f, err := os.Open(fn)
		if err == nil {
			bytes := make([]byte, 4)
			count, err := f.Read(bytes)
			f.Close()
			if (err == nil) && (count == 4) {
				if string(bytes) == "GRIB" {
					storeResult(thisIndex, forecast, "ok", fn)
				} else {
					log.Printf("#%2d Hour %d Not a GRIB: %s\n", thisIndex, forecast, fn)
					if verbose {
						log.Printf("URL: %s\n", url)
						// print the contents of the file - should be error msg
						f, _ = os.Open(fn) // it opened above, should open now
						bytes = make([]byte, 10240)
						f.Read(bytes)
						f.Close()
						log.Printf("\n%s\n", string(bytes))
					}
					_ = os.Remove(fn)
					storeResult(thisIndex, forecast, "bad", "")
					if inProgress {
						mu.Lock()
						// Stop other threads from starting another fetch
						nextForecast = len(forecasts)
						mu.Unlock()
					}
				}
			} else {
				log.Printf("Couldn't read GRIB 4-byte data header: %s\n", fn)
				_ = os.Remove(fn)
				storeResult(thisIndex, forecast, "bad", "")
			}
		} else {
			log.Printf("Couldn't open: %s\n", fn)
			_ = os.Remove(fn)
			storeResult(thisIndex, forecast, "bad", "")
		}

	}
}

func fetch() {
	levels := ""
	modelLevels := variableSets[Z.variables].levels
	if len(modelLevels) == 1 && modelLevels[0] == "all" {
	        levels = "&all_lev=on"
        } else {
		// horribly inefficient - building the string this way is n^2
	        for _, s := range modelLevels {
		        levels = fmt.Sprintf("%s&lev_%s=on", levels, s)
		}
	}
	vars := ""
	modelVars := variableSets[Z.variables].variables
        if len(modelVars) == 1 && modelVars[0] == "all" {
	   vars = "&all_var=on"
	} else {
		// horribly inefficient - building the string this way is n^2
		for _, s := range modelVars {
			vars = fmt.Sprintf("%s&var_%s=on", vars, s)
	       }
	}

	startMonotonic := time.Now()
	start := startMonotonic.Round(0)
	utc := start.UTC()

	if Z.geo == "sf96" {
		M.horizon = "96h" // Adjust GFS (default 384) for shorter horizon - should change M.endLag, too
	}

	startLag, _ := time.ParseDuration(M.start)
	endLag, _ := time.ParseDuration(M.end)
	modelFrequency, _ := time.ParseDuration(M.modelFrequency)
	forecastFrequency, _ := time.ParseDuration(M.forecastFrequency)
	first := utc.Add(-startLag)
	if prev {
		first = first.Add(-modelFrequency)
	}
	zulu = first.Truncate(modelFrequency) // Model run Zulu time
	forecastLast := zulu.Add(endLag)
	inProgress = utc.Before(forecastLast)
	var inProgressZulu time.Time
	inProgressZulu = zulu

	if inProgress && !partial {
		// If the current run is in progress go back to the last complete run.
		// If prev is true inProgress will always be false
		first = first.Add(-modelFrequency)
		zulu = first.Truncate(modelFrequency) // Model run Zulu time
	}

	run := fmt.Sprintf("%04d-%02d-%02d_%02dz_%s_%s", zulu.Year(), int(zulu.Month()), zulu.Day(), zulu.Hour(), Z.geo, Z.model)
	log.Printf("Run: %s\n", run)
	if inProgress {
		local := forecastLast.Local()
		log.Printf("%s %02dz run in progress - last should be complete at %02d:%02d\n", Z.model, inProgressZulu.Hour(), local.Hour(), local.Minute())
	}

	usr, err := user.Current()
	baseDir := usr.HomeDir + "/Downloads/gribs"
	grb2Dir := baseDir + "/grb2"
	runDir := grb2Dir + "/" + run
	grb2 := grb2Dir + "/" + run + ".grb2"

	_, err = os.Stat(grb2)
	noGrb2 := (err != nil)
	_, err = os.Stat(runDir)
	noRunDir := (err != nil)

	if verbose {
		log.Printf("noGrb2: %v noRunDir: %v\n", noGrb2, noRunDir)
	}

	if !noGrb2 && noRunDir && !refetch {
		log.Printf("This complete model run exists in %s\n", grb2)
		log.Printf("Use -refetch to fetch again\n")
		nextFirst := zulu.Add(modelFrequency).Add(startLag).Local()
		nextLast := forecastLast.Add(modelFrequency).Local()
		log.Printf("The next model run first forecast should appear at %02d:%02d and be complete at %02d:%02d\n", nextFirst.Hour(), nextFirst.Minute(), nextLast.Hour(), nextLast.Minute())
		forecastLast = zulu.Add(modelFrequency)
		os.Exit(1)
	}

	if !noGrb2 {
		// Remove if refetching or merging from a previous partial fetch
		_ = os.Remove(grb2)
	}

	if noRunDir { // Forecast directory doesn't exist
		if verbose {
			log.Printf("Creating forecast directory %s\n", runDir)
		}
		_ = os.MkdirAll(runDir, 0755)
	} else { // Directory exists
		if verbose {
			log.Printf("Forecast directory exists %s\n", runDir)
		}

		if !merge && !refetch {
			log.Printf("Directory exists: %s\n", runDir)
			log.Printf("Use -merge to fetch missing forecasts\n")
			log.Printf("Use -refetch to overwwrite existing forecasts\n")
			os.Exit(-1)
		}

		if refetch { // Delete previous grb files and any other cruft
			_ = os.RemoveAll(runDir)
			_ = os.MkdirAll(runDir, 0755)
		}
	}

	// We have a start time and all directories are in place. Fetch the gribs.
	//	forecast0 := zulu.Add(startLag)
	hours := time.Duration(0)
	horizon, _ := time.ParseDuration(M.horizon)

	// Make a slice with all of the forecasts for this model run
	for hours <= horizon {
		forecast := int(hours.Hours())
		if (Z.model == "gfs") && (forecast > 240) && (forecast%12 != 0) {
			// Skip this forecast is not in the model run - gfs goes every 12 hours after 10 days
		} else {
			forecasts = append(forecasts, forecast)
		}
		hours += forecastFrequency
	}
	results = make([]result, len(forecasts))

	// Create goroutines to fetch N URLs concurrently
	wg.Add(threads)
	for i := 0; i < threads; i++ {
		go fetchForecasts(i, levels, vars, runDir)
	}
	wg.Wait() // Wait for the goroutines to complete

	// At this point the forecasts are in the files named in the results[] slice
	goodGribCount := 0
	skipGribCount := 0
	badGribCount := 0
	var gribs []string
	badGribs := ""

	for i, r := range results {
		if results[i].result == "ok" {
			goodGribCount++
			gribs = append(gribs, r.filename)
		} else if r.result == "exists" {
			skipGribCount++
			gribs = append(gribs, r.filename)
		} else if r.result == "bad" {
			badGribs = fmt.Sprintf("%s %d", badGribs, r.forecast)
			badGribCount++
		} else {
			if verbose {
				log.Printf("Unknown result for index #%d", i)
			}
		}
	}

	log.Printf("Good GRIBS: %d (%d fetched + %d previous) Bad: %d\n", goodGribCount+skipGribCount, goodGribCount, skipGribCount, badGribCount)
	if badGribCount > 0 {
		log.Printf("Could not fetch%s\n", badGribs)
		log.Printf("Use --merge to fetch missing forecasts\n")
		log.Printf("Use --prev for last complete model run\n")
		if inProgress {
			done := forecastLast.Local()
			log.Printf("Model run in progress. All forecasts should be available by %02d:%02d\n", done.Hour(), done.Minute())
		}
	}

	if (goodGribCount == 0) && (badGribCount == 0) {
		// No gribs fetched - tell user when next model run happens
		nextStart := first.Add(modelFrequency).Local()
		nextEnd := forecastLast.Add(modelFrequency).Local()
		log.Printf("No GRIBs fetched. Next model run starts at %02d:%02d and ends at %02d:%02d\n", nextStart.Hour(), nextStart.Minute(), nextEnd.Hour(), nextEnd.Minute())
	}

	// Fetched at least one new GRIB. Make a composite by catting them together
	if goodGribCount > 0 {
		// Create the outputfile
		out, err := os.Create(grb2)
		if err != nil {
			log.Fatal(err)
		}

		for _, fc := range gribs {
			f, err := os.Open(fc)
			if err != nil {
				log.Printf("Open failed: %s\n", fc)
				log.Fatal(err)
			}
			bytes, err := io.Copy(out, f)
			if err != nil {
				log.Printf("Copy failed: %s\n", fc)
				log.Fatal(err)
			}
			if verbose {
				log.Printf("%s: %d bytes\n", fc, bytes)
			}
			_ = f.Close()
		}
		_ = out.Close()
		st, _ := os.Stat(grb2)
		log.Printf("GRIB %s %s (%d bytes)\n", grb2, prettyInt(st.Size()), st.Size())
		if !keep && (badGribCount == 0) {
			// Delete the individual forecasts if this was a complete fetch
			if verbose {
				log.Printf("Cleaning up: %s\n", runDir)
			}
			os.RemoveAll(runDir)
		}
	}

	finish := time.Now()
	elapsed := time.Since(start)
	log.Printf("Fetch finished @ %02d:%02d, elapsed %d:%02d:%02d\n", finish.Hour(), finish.Minute(), int64(elapsed.Hours()), int64(elapsed.Minutes())%60, int64(elapsed.Seconds())%60)
}

func Usage() {
	fmt.Printf("Fetch NOAA weather models from the NOMADS repository. By default fetch the latest complete model.\n\n")
	flag.Usage() // Print help for args and flags

	// Print list of zones sorted alphabetically just for consistency
	fmt.Printf("Where region is one of:\n")
	var si []string
	for id, _ := range zones {
		si = append(si, id)
	}
	sort.Strings(si)
	for _, id := range si {
		fmt.Printf("%12s %v\n", id, zones[id].description)
	}

	os.Exit(1)
}

func args() {
	//	args := os.Args[1:]
	flag.BoolVar(&prev, "prev", false, "Fetch previous run")
	flag.BoolVar(&partial, "partial", false, "Fetch current in-progress model (if any)")
	flag.BoolVar(&merge, "merge", false, "Fetch missing forecasts")
	flag.BoolVar(&refetch, "refetch", false, "Refetch all forecasts for this run")
	flag.BoolVar(&keep, "keep", false, "Keep forecast directory after complete model fetch (default is to delete)")
	flag.IntVar(&threads, "threads", 4, "# of concurrent HTTP connections")
	flag.StringVar(&zone, "region", "", "Model & Area to fetch")
	flag.BoolVar(&verbose, "verbose", false, "Verbose")
	flag.BoolVar(&help, "help", false, "Print usage message")
	flag.Parse()

	if help {
		Usage()
	}
	
	if zone == "" {
		fmt.Printf("No zone specified\n")
		Usage()
	}

	var ok bool
	Z, ok = zones[zone]
	if !ok {
		fmt.Printf("Unknown zone: %v\n", zone)
		Usage()
	}

	if refetch && merge {
		fmt.Printf("Specify only one of merge & refetch\n")
		Usage()
	}

	if verbose {
		log.Printf("Args zone: %v, prev: %v, merge: %v, refetch: %v keep: %v verbose: %v\n", zone, prev, merge, refetch, keep, verbose)
	}
}

func main() {
	args()
	log.Printf("Fetching zone %v model %s west %5.2f east %5.2f north %5.2f south %5.2f\n", zone, Z.model, Z.longitude.west, Z.longitude.east, Z.latitude.north, Z.latitude.south)
	var ok bool
	M, ok = models[Z.model]
	if !ok {
		log.Printf("Zone %s has no associated model '%s'\n", zone, Z.model)
		os.Exit(-1)
	}
	fetch()
}
