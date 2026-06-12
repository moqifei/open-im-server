//go:build mage
// +build mage

package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/openimsdk/gomake/mageutil"
)

func getVersion() string {
	data, err := os.ReadFile("version/version")
	if err != nil {
		return "dev"
	}
	return strings.TrimSpace(string(data))
}

var Default = Build

var Aliases = map[string]any{
	"buildcc": BuildWithCustomConfig,
	"startcc": StartWithCustomConfig,
}

// Build support specifical binary build.
func Build() {
	flag.Parse()
	bin := flag.Args()
	if len(bin) != 0 {
		bin = bin[1:]
	}
	fmt.Println("Building binaries...")
	mageutil.Build(bin)
}

func BuildWithCustomConfig() {
	flag.Parse()
	bin := flag.Args()
	if len(bin) != 0 {
		bin = bin[1:]
	}
	fmt.Println("Building binaries with custom config...")
	mageutil.Build(bin)
}

func Start() {
	mageutil.InitForSSC()
	err := setMaxOpenFiles()
	if err != nil {
		mageutil.PrintRed("setMaxOpenFiles failed " + err.Error())
		os.Exit(1)
	}
	fmt.Println("Starting...")
	mageutil.StartToolsAndServices()
}

func StartWithCustomConfig() {
	mageutil.InitForSSC()
	err := setMaxOpenFiles()
	if err != nil {
		mageutil.PrintRed("setMaxOpenFiles failed " + err.Error())
		os.Exit(1)
	}
	fmt.Println("Starting with custom config...")
	mageutil.StartToolsAndServices()
}

func Stop() {
	fmt.Println("Stopping...")
	mageutil.StopAndCheckBinaries()
}

func Check() {
	fmt.Println("Checking binaries...")
	mageutil.CheckAndReportBinariesStatus()
}

// Export is not available in this gomake version; use the official release pipeline instead.
