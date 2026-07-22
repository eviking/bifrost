package main

import (
	"os"

	"github.com/grafana/grafana-plugin-sdk-go/backend/datasource"
	"github.com/grafana/grafana-plugin-sdk-go/backend/log"

	"bifrost-datafusion-datasource/pkg/plugin"
)

func main() {
	if err := datasource.Manage("bifrost-datafusion-datasource", plugin.NewDatasource, datasource.ManageOpts{}); err != nil {
		log.DefaultLogger.Error("plugin exited with error", "error", err)
		os.Exit(1)
	}
}
