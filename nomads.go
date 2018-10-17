package main

import "os"
import "os/user"
import "os/exec"
import "syscall"
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
	modelLevels []string
	modelVars   []string
}

var zones = map[string]Zone{
	"sf": Zone{
		description: "SF Bay Wind hi-res (18 hour hrrr)",
		geo:         "sf",
		model:       "hrrr",
		longitude:   Longitude{-123.0, -122.0},
		latitude:    Latitude{38.0, 37.0},
		modelLevels: []string{"surface", "2_m_above_ground", "10_m_above_ground"},
		modelVars:   []string{"PRES", "UGRD", "VGRD", "TMP", "WIND", "GUST"},
	},
	"sf36": Zone{
		description: "SF Bay Wind hi-res (36 hour hrrr, runs every 6 hours)",
		geo:         "sf",
		model:       "hrrr36",
		longitude:   Longitude{-123, -122},
		latitude:    Latitude{38, 37},
		modelLevels: []string{"surface", "2_m_above_ground", "10_m_above_ground"},
		modelVars:   []string{"PRES", "UGRD", "VGRD", "TMP", "WIND", "GUST"},
	},
	"sfoffshore": Zone{
		description: "SF Bay & Farallones Wind hi-res (18 hour hrrr)",
		geo:         "hrrr",
		model:       "sfoffshore",
		longitude:   Longitude{-131, -119},
		latitude:    Latitude{41, 35},
		modelLevels: []string{"surface", "2_m_above_ground", "10_m_above_ground"},
		modelVars:   []string{"PRES", "UGRD", "VGRD", "TMP", "WIND", "GUST"},
	},
	"sfoffshore36": Zone{
		description: "SF Bay & Farallones Wind hi-res (36 hour hrrr)",
		geo:         "sfoffshore",
		model:       "hrrr36",
		longitude:   Longitude{-131, -119},
		latitude:    Latitude{41, 35},
		modelLevels: []string{"surface", "2_m_above_ground", "10_m_above_ground"},
		modelVars:   []string{"PRES", "UGRD", "VGRD", "TMP", "WIND", "GUST"},
	},
	"sfsub": Zone{
		description: "SF Bay Wind hi-res, 15m intervals (18 hour hrrr_sub)",
		geo:         "sf",
		model:       "hrrr_sub",
		longitude:   Longitude{-123, -122},
		latitude:    Latitude{38, 37},
		modelLevels: []string{"surface", "2_m_above_ground", "10_m_above_ground"},
		modelVars:   []string{"PRES", "UGRD", "VGRD", "TMP", "WIND", "GUST"},
	},
	"sf96": Zone{
		description: "SF Bay Wind (96 hour GFS)",
		geo:         "sf96",
		model:       "gfs_hourly",
		longitude:   Longitude{-124.5, -122},
		latitude:    Latitude{38.5, 36.5},
		modelLevels: []string{"mean_sea_level", "surface", "2_m_above_ground", "10_m_above_ground"},
		modelVars:   []string{"PRMSL", "UGRD", "VGRD", "TMP", "GUST"},
	},
	"sfnam": Zone{
		description: "Outside SF Bay Wind (60 hour NAM)",
		geo:         "sfnam",
		model:       "nam-nest",
		longitude:   Longitude{-124.5, -122},
		latitude:    Latitude{38.5, 36.5},
		modelLevels: []string{"mean_sea_level", "surface", "2_m_above_ground", "10_m_above_ground"},
		modelVars:   []string{"PRMSL", "UGRD", "VGRD", "TMP", "GUST"},
	},
	"canam": Zone{
		description: "California Coast (60 hour NAM)",
		geo:         "ca",
		model:       "nam-nest",
		longitude:   Longitude{-130.0, -116.0},
		latitude:    Latitude{42.0, 32.5},
		modelLevels: []string{"mean_sea_level", "surface", "2_m_above_ground", "10_m_above_ground"},
		modelVars:   []string{"PRMSL", "UGRD", "VGRD", "TMP", "GUST"},
	},
	"cahrrr": Zone{
		description: "California Coast (18 hour HRRR)",
		geo:         "ca",
		model:       "hrrr",
		longitude:   Longitude{-130.0, -116.0},
		latitude:    Latitude{42.0, 32.5},
		modelLevels: []string{"surface", "mean_sea_level", "surface", "2_m_above_ground", "10_m_above_ground"},
		modelVars:   []string{"GUST", "TMP", "UGRD", "VGRD", "WIND"},
	},
	"cahrrr36": Zone{
		description: "California Coast (36 hour HRRR)",
		geo:         "ca",
		model:       "hrrr36",
		longitude:   Longitude{-130.0, -116.0},
		latitude:    Latitude{42.0, 32.5},
		modelLevels: []string{"surface", "2_m_above_ground", "10_m_above_ground"},
		modelVars:   []string{"PRES", "UGRD", "VGRD", "TMP", "WIND", "GUST"},
	},
	"fire": Zone{
		description: "SF North Bay fire (18 hour hrrr_sub)",
		geo:         "fire",
		model:       "hrrr_sub",
		longitude:   Longitude{-123, -121},
		latitude:    Latitude{39, 37},
		modelLevels: []string{"surface", "2_m_above_ground", "10_m_above_ground"},
		modelVars:   []string{"PRES", "UGRD", "VGRD", "TMP", "WIND", "GUST"},
	},
	"tahoe": Zone{
		description: "Tahoe area (18 hour hrrr)",
		geo:         "tahoe",
		model:       "hrrr",
		longitude:   Longitude{-121, -119},
		latitude:    Latitude{40, 38},
		modelLevels: []string{"mean_sea_level", "surface", "2_m_above_ground", "10_m_above_ground", "entire_atmosphere", "entire_atmosphere_%5C%28considered_as_a_single_layer%5C%29"},
		modelVars:   []string{"APCP", "GUST", "PRATE", "PRES", "PWAT", "TMP", "UGRD", "VGRD", "WIND"},
		//		modelVars:   []string{"GUST", "TMP", "UGRD", "VGRD", "WIND"},
	},
	"pacific": Zone{
		description: "North Pacific Wind/Precip (10 day GFS)",
		geo:         "pacific",
		model:       "gfs",
		longitude:   Longitude{-230, -100},
		latitude:    Latitude{70, 10},
		modelLevels: []string{"mean_sea_level", "surface", "2_m_above_ground", "10_m_above_ground", "entire_atmosphere_%5C%28considered_as_a_single_layer%5C%29"},
		modelVars:   []string{"PRMSL", "UGRD", "VGRD", "TMP", "APCP", "PWAT", "PRATE", "GUST"},
	},
	"paccup": Zone{
		description: "North-East Pacific Wind (10 day GFS)",
		geo:         "paccup",
		model:       "gfs",
		longitude:   Longitude{-160, -115},
		latitude:    Latitude{50, 15},
		modelLevels: []string{"mean_sea_level", "surface", "2_m_above_ground", "10_m_above_ground"},
		modelVars:   []string{"PRMSL", "UGRD", "VGRD", "GUST", "PRES"},
	},
	"se": Zone{
		description: "IOD Sweden Race Ares",
		geo:         "stenungsund",
		model:       "gfs",
		longitude:   Longitude{0, 20},
		latitude:    Latitude{65, 50},
		modelLevels: []string{"mean_sea_level", "surface", "2_m_above_ground", "10_m_above_ground"},
		modelVars:   []string{"PRMSL", "UGRD", "VGRD", "TMP", "CAPE"},
	},
	"volvo": Zone{
		description: "Wherever the Volvo Ocean Race is",
		geo:         "volvo",
		model:       "gfs",
		longitude:   Longitude{140, 170},
		latitude:    Latitude{0, -40},
		modelLevels: []string{"mean_sea_level", "surface", "2_m_above_ground", "10_m_above_ground"},
		modelVars:   []string{"PRMSL", "UGRD", "VGRD", "TMP", "CAPE"},
	},
	"utah": Zone{
		description: "Big Sky to Wasatch",
		geo:         "utah",
		model:       "hrrr",
		longitude:   Longitude{-118, -105},
		latitude:    Latitude{48, 38},
		modelLevels: []string{"mean_sea_level", "surface", "2_m_above_ground", "10_m_above_ground", "entire_atmosphere", "entire_atmosphere_%5C%28considered_as_a_single_layer%5C%29"},
		modelVars:   []string{"APCP", "GUST", "PRATE", "PRES", "PWAT", "TMP", "UGRD", "VGRD", "WIND"},
	},
	"chessy": Zone{
		description: "Annapolis Wind hi-res (18 hour hrrr)",
		geo:         "chesapeake",
		model:       "hrrr",
		longitude:   Longitude{-77.0, -75.5},
		latitude:    Latitude{39.75, 38.5},
		modelLevels: []string{"surface", "2_m_above_ground", "10_m_above_ground"},
		modelVars:   []string{"PRES", "UGRD", "VGRD", "TMP", "WIND", "GUST"},
	},
	"newport": Zone{
		description: "Newport Wind hi-res (18 hour hrrr)",
		geo:         "newport",
		model:       "hrrr",
		longitude:   Longitude{-71.5, -71.0},
		latitude:    Latitude{41.75, 41.25},
		modelLevels: []string{"surface", "2_m_above_ground", "10_m_above_ground"},
		modelVars:   []string{"PRES", "UGRD", "VGRD", "TMP", "WIND", "GUST"},
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
		baseurl:           "http://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl?file=%s%s%s&subregion=&leftlon=%5.2f&rightlon=%5.2f&toplat=%5.2f&bottomlat=%5.2f&dir=%%2Fgfs.%04d%02d%02d%02d",
		baseurlfn:         "%s.t%02dz.pgrb2.0p25.f%03d",
	},
	"gfs_hourly": {
		fn:                "gfs",
		modelFrequency:    "6h",
		forecastFrequency: "1h",
		horizon:           "384h",
		start:             "3.5h", // gfs forecasts show up about 3 1/2  hours after model run
		end:               "5h",   // gfs 384 hour forecast completes about five hours after model run
		baseurl:           "http://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25_1hr.pl?file=%s%s%s&subregion=&leftlon=%5.2f&rightlon=%5.2f&toplat=%5.2f&bottomlat=%5.2f&dir=%%2Fgfs.%04d%02d%02d%02d",
		baseurlfn:         "%s.t%02dz.pgrb2.0p25.f%03d",
	},
	"hrrr": {
		fn:                "hrrr",
		modelFrequency:    "1h",  // hrrr runs every hour
		forecastFrequency: "1h",  // forecasts are one hour apart
		horizon:           "18h", // hrrr is 18 hour forecast; for the 4 times a day it's longer use hrrr36
		start:             "50m", // hrrr f00 50 minutes after the hour
		end:               "85m", // f18 a bit more than 1/2 hour later
		baseurl:           "http://nomads.ncep.noaa.gov/cgi-bin/filter_hrrr_2d.pl?file=%s%s%s&subregion=&leftlon=%5.2f&rightlon=%5.2f&toplat=%5.2f&bottomlat=%5.2f&dir=%%2Fhrrr.%04d%02d%02d%%2Fconus",
		baseurlfn:         "%s.t%02dz.wrfsfcf%02d.grib2",
	},
	"hrrr36": {
		fn:                "hrrr",
		modelFrequency:    "6h",   // hrrr runs every hour, but every six hours the forecast is extended to 36 hours
		forecastFrequency: "1h",   // forecasts are one hour apart
		horizon:           "36h",  // how many hours of forecast to fetch
		start:             "50m",  // hrrr f00 50 minutes after the hour
		end:               "110m", // f36 usually an hour after f00
		baseurl:           "http://nomads.ncep.noaa.gov/cgi-bin/filter_hrrr_2d.pl?file=%s%s%s&subregion=&leftlon=%5.2f&rightlon=%5.2f&toplat=%5.2f&bottomlat=%5.2f&dir=%%2Fhrrr.%04d%02d%02d%%2Fconus",
		baseurlfn:         "%s.t%02dz.wrfsfcf%02d.grib2",
	},
	"hrrr_sub": { // Same as hrrr but has 15 minute sub-hourly forecasts
		fn:                "hrrr",
		modelFrequency:    "1h",
		forecastFrequency: "1h",
		horizon:           "18h",
		start:             "55m", // hrrr_sub f00 55 minutes after the hour
		end:               "85m", // f18 usually 25 - 30 minutes later
		baseurl:           "http://nomads.ncep.noaa.gov/cgi-bin/filter_hrrr_sub.pl?file=%s%s%s&subregion=&leftlon=%5.2f&rightlon=%5.2f&toplat=%5.2f&bottomlat=%5.2f&dir=%%2Fhrrr.%04d%02d%02d%%2Fconus",
		baseurlfn:         "%s.t%02dz.wrfsubhf%02d.grib2",
	},
	"nam": {
		fn:                "nam",
		modelFrequency:    "6h",   // hours between model runs - assume all run at 00z
		forecastFrequency: "1h",   // hours between forecast steps
		horizon:           "60h",  // NAM goes out 60 hours
		start:             "1.5h", // NAM forecasts show up about 1 1/2 hours after model run
		end:               "3h",   // NAM 60 hour forecast completes about three hours after model run
		baseurl:           "http://nomads.ncep.noaa.gov/cgi-bin/filter_nam.pl?file=%s%s%s&subregion=&leftlon=%5.2f&rightlon=%5.2f&toplat=%5.2f&bottomlat=%5.2f&dir=%%2Fnam.%04d%02d%02d",
		baseurlfn:         "%s.t%02dz.awphys%02d.tm00.grib2",
	},
	"nam-nest": {
		fn:                "nam",
		modelFrequency:    "6h",
		forecastFrequency: "1h",
		horizon:           "60h",
		start:             "1.5h",
		end:               "3h",
		baseurl:           "http://nomads.ncep.noaa.gov/cgi-bin/filter_nam_conusnest.pl?file=%s%s%s&subregion=&leftlon=%5.2f&rightlon=%5.2f&toplat=%5.2f&bottomlat=%5.2f&dir=%%2Fnam.%04d%02d%02d",
		baseurlfn:         "%s.t%02dz.conusnest.hiresf%02d.tm00.grib2",
	},
}

var zone string
var prev bool
var merge bool
var refetch bool
var keep bool
var verbose bool
var Z Zone
var M Model

func prettyInt(i int64) (string) {
	const (Kilo int64 = 1024
		Mega = 1024 * Kilo
		Giga = 1024 * Mega
		Tera = 1024 * Giga
		Peta = 1024 * Tera )
	if (i > Peta) {
		return(fmt.Sprintf("%.2fPiB", float64(i) / float64(Peta)))
	}
	if (i > Tera) {
		return(fmt.Sprintf("%.2fTiB", float64(i) / float64(Tera)))
	}
	if (i > Giga) {
		return(fmt.Sprintf("%.2fGiB", float64(i) / float64(Giga)))
	}
	if (i > Mega) {
		return(fmt.Sprintf("%.2fMiB", float64(i) / float64(Mega)))
	}
	if (i > Kilo) {
		return(fmt.Sprintf("%.2fKiB", float64(i) / float64(Kilo)))
	}
	return(fmt.Sprintf("%4dB", i))
	
}

func fetchUrl(url, fn string) (exitCode int) {
	if verbose {
		fmt.Printf("%s %s %s %s\n", "curl", "-o", fn, url)
	}
	cmd := exec.Command("curl", "-o", fn, url)
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

func fetch() {
	levels := ""
	for _, s := range Z.modelLevels {
		levels = fmt.Sprintf("%s&lev_%s=on", levels, s) // horribly inefficient
	}
	vars := ""
	for _, s := range Z.modelVars {
		vars = fmt.Sprintf("%s&var_%s=on", vars, s) // horribly inefficient
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
	zulu := first.Truncate(modelFrequency) // Model run Zulu time
	forecastLast := zulu.Add(endLag)
	inProgress := utc.Before(forecastLast)

	run := fmt.Sprintf("%04d-%02d-%02d_%02dz_%s_%s", zulu.Year(), int(zulu.Month()), zulu.Day(), zulu.Hour(), Z.geo, Z.model)
	fmt.Printf("Run: %s\n", run)
	if inProgress {
		local := forecastLast.Local()
		fmt.Printf("Model run in progress. Last forecast should be available at %02d:%02d\n", local.Hour(), local.Minute())
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
		fmt.Printf("noGrb2: %v noRunDir: %v\n", noGrb2, noRunDir)
	}

	if !noGrb2 && noRunDir && !refetch {
		fmt.Printf("This complete model run exists in %s\n", grb2)
		fmt.Printf("Use -refetch to fetch again\n")
		os.Exit(1)
	}

	if !noGrb2 {
		// Remove if refetching or merging from a previous partial fetch
		_ = os.Remove(grb2)
	}

	if noRunDir { // Forecast directory doesn't exist
		if verbose {
			fmt.Printf("Creating forecast directory %s\n", runDir)
		}
		_ = os.MkdirAll(runDir, 0755)
	} else { // Directory exists
		if verbose {
			fmt.Printf("Forecast directory exists %s\n", runDir)
		}

		if !merge && !refetch {
			fmt.Fprintf(os.Stderr, "Directory exists: %s\n", runDir)
			fmt.Fprintf(os.Stderr, "Use -merge to fetch missing forecasts\n")
			fmt.Fprintf(os.Stderr, "Use -refetch to overwwrite existing forecasts\n")
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

	knownCurlErrors := map[int]string{
		7:  "connection timed out",
		18: "connection closed with data remaining",
		56: "connection reset",
	}

	goodGribCount := 0
	badGribCount := 0
	skipGribCount := 0
	
	var gribs []string
	badGribs := ""
	for hours <= horizon {
		forecast := int(hours.Hours())
		if verbose {
			fmt.Printf("Start fetching forecast %d\n", forecast)
		}

		var url string
		urlfn := fmt.Sprintf(M.baseurlfn, M.fn, zulu.Hour(), forecast)
		if verbose {
			fmt.Printf("urlfn: %s\n", urlfn)
		}

		// GFS has one directory per day, others one directory per model run
		if (Z.model == "gfs") || (Z.model == "gfs_hourly") {
			url = fmt.Sprintf(M.baseurl, urlfn, levels, vars, Z.longitude.west, Z.longitude.east, Z.latitude.north, Z.latitude.south, zulu.Year(), int(zulu.Month()), zulu.Day(), zulu.Hour())
		} else {
			url = fmt.Sprintf(M.baseurl, urlfn, levels, vars, Z.longitude.west, Z.longitude.east, Z.latitude.north, Z.latitude.south, zulu.Year(), int(zulu.Month()), zulu.Day())
		}

		fn := runDir + "/" + urlfn

		if (Z.model == "gfs") && (forecast > 240) && (forecast%12 != 0) {
			fmt.Printf("Not forecast %s\n", urlfn)
			hours += forecastFrequency
			continue
		}

		_, err := os.Stat(fn)
		if err == nil {
			fmt.Printf("Skip (exists) %s\n", urlfn)
			skipGribCount++
			gribs = append(gribs, fn)
			hours += forecastFrequency
			continue
		}

		fmt.Printf("Fetching %s\n", urlfn)
		ok := true
		attempts := 0
		errCode := 1
		for (errCode != 0) && (attempts <= 5) && (ok) {
			attempts += 1
			if attempts > 1 {
				fmt.Printf("Curl attempt #%d %v\n", attempts, urlfn)
			}
			if errCode = fetchUrl(url, fn); errCode != 0 {
				if verbose {
					fmt.Printf("fetchUrl returns %v\n", errCode)
				}
				fault, ok := knownCurlErrors[errCode]
				if ok {
					fmt.Printf("curl: failure %d: %s\n", errCode, fault)
				} else {
					fmt.Printf("curl: failure %d: %s\n", errCode, "Unexpected fault")
				}
			}
		}
		if !ok {
			_ = os.Remove(fn)
			os.Exit(2) // Exit if we couldn't fetch a file - something is wrong
		}

		// check to see if it's a GRIB
		f, err := os.Open(fn)
		if err == nil {
			bytes := make([]byte, 4)
			count, err := f.Read(bytes)
			f.Close()
			if (err == nil) && (count == 4) {
				if string(bytes) == "GRIB" {
					gribs = append(gribs, fn)
					goodGribCount++
				} else {
					badGribs += fmt.Sprintf(" %03d", forecast)
					badGribCount++
					fmt.Printf("Not a GRIB: %s\n", fn)
					if verbose {
						// print the contents of the file - should be error msg
						f, _ = os.Open(fn) // it opened above, should open now
						bytes = make([]byte, 10240)
						f.Read(bytes)
						f.Close()
						fmt.Printf("\n%s\n", string(bytes))
					}
					_ = os.Remove(fn)
					if inProgress {
						break // stop fetching after first failure if model is still running
					}
				}
			} else {
				fmt.Printf("Couldn't read GRIB 4-byte data header: %s\n", fn)
				_ = os.Remove(fn)
			}
		} else {
			fmt.Printf("Couldn't open: %s\n", fn)
			_ = os.Remove(fn)
		}

		hours += forecastFrequency
	}

	fmt.Printf("Good GRIBS: %d (%d fetched + %d previous) Bad: %d\n", goodGribCount + skipGribCount, goodGribCount, skipGribCount, badGribCount)
	if badGribCount > 0 {
		fmt.Printf("Could not fetch%s\n", badGribs)
		fmt.Printf("Use --merge to fetch missing forecasts\n")
		fmt.Printf("Use --prev for last complete model run\n")
		if inProgress {
			done := forecastLast.Local()
			fmt.Printf("Model run in progress. All forecasts should be available by %02d:%02d\n", done.Hour(), done.Minute())
		}
	}

	if (goodGribCount == 0) && (badGribCount == 0) {
		// No gribs fetched - tell user when next model run happens
		nextStart := first.Add(modelFrequency).Local()
		nextEnd := forecastLast.Add(modelFrequency).Local()
		fmt.Printf("No GRIBs fetched. Next model run starts at %02d:%02d and ends at %02d:%02d\n", nextStart.Hour(), nextStart.Minute(), nextEnd.Hour(), nextEnd.Minute())
	}

	if goodGribCount > 0 {
		// Cat the fetched gribs together making a composite GRIB
		out, err := os.Create(grb2)
		if err != nil {
			log.Fatal(err)
		}

		for _, fc := range gribs {
			f, err := os.Open(fc)
			if err != nil {
				fmt.Printf("Open failed: %s\n", fc)
				log.Fatal(err)
			}
			bytes, err := io.Copy(out, f)
			if err != nil {
				fmt.Printf("Copy failed: %s\n", fc)
				log.Fatal(err)
			}
			if verbose {
				fmt.Printf("%s: %d bytes\n", fc, bytes)
			}
			_ = f.Close()
		}
		_ = out.Close()
		st, _ := os.Stat(grb2)
		fmt.Printf("GRIB %s %s (%d bytes)\n", grb2, prettyInt(st.Size()), st.Size())
		if !keep && (badGribCount == 0) {
			// Delete the individual forecasts if this was a complete fetch
			if verbose {
				fmt.Printf("Cleaning up: %s\n", runDir)
			}
			os.RemoveAll(runDir)
		}
	}

	finish := time.Now()
	elapsed := time.Since(start)
	fmt.Printf("Fetch finished @ %02d:%02d, elapsed %d:%02d:%02d\n", finish.Hour(), finish.Minute(), int64(elapsed.Hours()), int64(elapsed.Minutes()) % 60, int64(elapsed.Seconds()) % 60)
}

func Usage() {
	flag.Usage() // Print help for args and flags

	// Print list of zones sorted alphabetically just for consistency
	fmt.Fprintf(os.Stderr, "Where zone is one of:\n")
	var si[]string
	for id, _ := range zones {
		si = append(si, id)
	}
	sort.Strings(si)
	for _, id := range si {
		fmt.Fprintf(os.Stderr, "%12s %v\n", id, zones[id].description)
	}
	
	os.Exit(1)
}

func args() {
	//	args := os.Args[1:]
	flag.BoolVar(&prev, "prev", false, "Fetch previous complete model run")
	flag.BoolVar(&merge, "merge", false, "Fetch missing forecasts")
	flag.BoolVar(&refetch, "refetch", false, "Refetch all forecasts")
	flag.BoolVar(&keep, "keep", false, "Keep forecasts after complete model fetch")
	flag.BoolVar(&verbose, "verbose", false, "Verbose")
	flag.StringVar(&zone, "zone", "", "Model & Area to fetch")
	flag.Parse()
	if zone == "" {
		fmt.Fprintf(os.Stderr, "No zone specified\n")
		Usage()
	}

	var ok bool
	Z, ok = zones[zone]
	if !ok {
		fmt.Fprintf(os.Stderr, "Unknown zone: %v\n", zone)
		Usage()
	}

	if refetch && merge {
		fmt.Fprintf(os.Stderr, "Specify only one of merge & refetch\n")
		Usage()
	}

	if verbose {
		fmt.Printf("Args zone: %v, prev: %v, merge: %v, refetch: %v keep: %v verbose: %v\n", zone, prev, merge, refetch, keep, verbose)
	}
}

func main() {
	args()
	fmt.Printf("Fetching zone %v model %s west %5.2f east %5.2f north %5.2f south %5.2f\n", zone, Z.model, Z.longitude.west, Z.longitude.east, Z.latitude.north, Z.latitude.south)
	var ok bool
	M, ok = models[Z.model]
	if !ok {
		fmt.Fprintf(os.Stderr, "Zone %s has no associated model '%s'\n", zone, Z.model)
		os.Exit(-1)
	}
	fetch()
}
