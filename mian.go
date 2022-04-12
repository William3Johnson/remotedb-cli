package main

import (
	"fmt"
	"os"

	"github.com/urfave/cli"
	c "github.com/bnb-chain/remotedb-cli/commands"
)

func main() {
	app := cli.NewApp()
	app.Name = "kvrocks.cli"
	app.Usage = "kvrocks cluster manage tool"

	app.Commands = []cli.Command{
		c.ClusterInitCommand,
	}

	arg := append(os.Args)
	for _, cmd := range app.Commands {
		if os.Args[1] == cmd.Name {
			app.Run(arg)
			os.Exit(0)
		}
	}

	if (len(os.Args) == 2 && (string(os.Args[1]) == "-h" || string(os.Args[1]) == "--help")) || (len(os.Args) == 1) {
		help := `Usage:
        remotedb-cli cluster.init -s <shard> -c <configfile>, -h for more details
        `
		fmt.Println(help)
		os.Exit(1)
	}
	return 
}