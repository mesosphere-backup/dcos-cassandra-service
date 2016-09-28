package main

import (
	"encoding/json"
	"fmt"
	"github.com/mesosphere/dcos-commons/cli"
	"gopkg.in/alecthomas/kingpin.v2"
	"log"
	"net/url"
	"os"
	"strings"
)

func main() {
	modName, err := cli.GetModuleName()
	if err != nil {
		log.Fatalf(err.Error())
	}
	// Prettify the user-visible service name:
	serviceName := strings.Title(modName) // eg 'Cassandra'
	if modName == "dse" {
		serviceName = "DSE"
	}

	app, err := cli.NewApp(
		"0.1.0",
		"Mesosphere",
		fmt.Sprintf("Deploy and manage %s clusters", serviceName))
	if err != nil {
		log.Fatalf(err.Error())
	}

	// Omit standard "connection" section. Cassandra isn't on the standard paths yet, and omit
	// standard "config" and "state" sections since Cassandra isn't installing
	// ConfigResource/StateResource yet.
	// Once these are fixed, this block can all be replaced with a call to "HandleCommonArgs"
	cli.HandleCommonFlags(app, modName, fmt.Sprintf("%s DC/OS CLI Module", serviceName))
	//cli.HandleConfigSection(app)
	//cli.HandleConnectionSection(app)
	cli.HandlePlanSection(app)
	//cli.HandleStateSection(app)

	handleSeedsCommand(app)
	handleCustomConnectionCommand(app, serviceName)

	handleNodeSection(app, serviceName)
	handleBackupRestoreSections(app, serviceName)
	handleCleanupRepairSections(app)

	// Omit modname:
	kingpin.MustParse(app.Parse(os.Args[2:]))
}

func handleSeedsCommand(app *kingpin.Application) {
	app.Command("seeds", "Retrieve seed node information").Action(
		func(c *kingpin.ParseContext) error {
			cli.PrintJSON(cli.HTTPGet("v1/seeds"))
			return nil
		})
}

type ConnectionHandler struct {
	showAddress bool
	showDns bool
}
func (cmd *ConnectionHandler) runBase(c *kingpin.ParseContext) error {
	if cmd.showAddress {
		cli.PrintJSON(cli.HTTPGet("v1/connection/address"))
	} else if cmd.showDns {
		cli.PrintJSON(cli.HTTPGet("v1/connection/dns"))
	} else {
		cli.PrintJSON(cli.HTTPGet("v1/connection"))
	}
	return nil
}
func handleCustomConnectionCommand(app *kingpin.Application, serviceName string) {
	cmd := &ConnectionHandler{}

	// Have three explicit commands, to ensure that each possibility shows up in --help:
	connection := app.Command("connection", fmt.Sprintf("Provides %s connection information", serviceName)).Action(cmd.runBase)
	connection.Flag("address", fmt.Sprintf("Provide addresses of the %s nodes", serviceName)).BoolVar(&cmd.showAddress)
	connection.Flag("dns", fmt.Sprintf("Provide dns names of the %s nodes", serviceName)).BoolVar(&cmd.showDns)
}


type NodeHandler struct {
	nodeId int
}
func (cmd *NodeHandler) runDescribe(c *kingpin.ParseContext) error {
	cli.PrintJSON(cli.HTTPGet(fmt.Sprintf("v1/nodes/node-%d/info", cmd.nodeId)))
	return nil
}
func (cmd *NodeHandler) runList(c *kingpin.ParseContext) error {
	cli.PrintJSON(cli.HTTPGet("v1/nodes/list"))
	return nil
}
func (cmd *NodeHandler) runReplace(c *kingpin.ParseContext) error {
	query := url.Values{}
	query.Set("node", fmt.Sprintf("node-%d", cmd.nodeId))
	cli.PrintJSON(cli.HTTPPutQuery("v1/nodes/replace", query.Encode()))
	return nil
}
func (cmd *NodeHandler) runRestart(c *kingpin.ParseContext) error {
	query := url.Values{}
	query.Set("node", fmt.Sprintf("node-%d", cmd.nodeId))
	cli.PrintJSON(cli.HTTPPutQuery("v1/nodes/restart", query.Encode()))
	return nil
}
func (cmd *NodeHandler) runStatus(c *kingpin.ParseContext) error {
	cli.PrintJSON(cli.HTTPGet(fmt.Sprintf("v1/nodes/node-%d/status", cmd.nodeId)))
	return nil
}
func handleNodeSection(app *kingpin.Application, serviceName string) {
	cmd := &NodeHandler{}
	connection := app.Command("node", fmt.Sprintf("Manage %s nodes", serviceName))

	describe := connection.Command("describe", "Describes a single node").Action(cmd.runDescribe)
	describe.Arg("node_id", "The node id to describe").IntVar(&cmd.nodeId)

	connection.Command("list", "Lists all nodes").Action(cmd.runList)

	replace := connection.Command("replace", "Replaces a single node job, moving it to a different agent").Action(cmd.runReplace)
	replace.Arg("node_id", "The node id to replace").IntVar(&cmd.nodeId)

	restart := connection.Command("restart", "Restarts a single node job, keeping it on the same agent").Action(cmd.runRestart)
	restart.Arg("node_id", "The node id to restart").IntVar(&cmd.nodeId)

	status := connection.Command("status", "Gets the status of a single node").Action(cmd.runStatus)
	status.Arg("node_id", "The node id to check").IntVar(&cmd.nodeId)
}

// Reuse same struct for both 'backup start' and 'restore start' (same args)
type BackupRestoreHandler struct {
	backupName string
	externalLocation string
	s3AccessKey string
	s3SecretKey string
	azureAccount string
	azureKey string
}
func (cmd *BackupRestoreHandler) getArgs() map[string]interface{} {
	return map[string]interface{} {
		"backup_name": cmd.backupName,
		"external_location": cmd.externalLocation,
		"s3_access_key": cmd.s3AccessKey,
		"s3_secret_key": cmd.s3SecretKey,
		"azure_account": cmd.azureAccount,
		"azure_key": cmd.azureKey,
	}
}
func (cmd *BackupRestoreHandler) runBackup(c *kingpin.ParseContext) error {
	payload, err := json.Marshal(cmd.getArgs())
	if err != nil {
		return err
	}
	cli.HTTPPutJSON("v1/backup/start", string(payload))
	return nil
}
func (cmd *BackupRestoreHandler) runBackupStop(c *kingpin.ParseContext) error {
	cli.HTTPPut("v1/backup/stop")
	return nil
}
func (cmd *BackupRestoreHandler) runRestore(c *kingpin.ParseContext) error {
	payload, err := json.Marshal(cmd.getArgs())
	if err != nil {
		return err
	}
	cli.HTTPPutJSON("v1/restore/start", string(payload))
	return nil
}
func (cmd *BackupRestoreHandler) runRestoreStop(c *kingpin.ParseContext) error {
	cli.HTTPPut("v1/restore/stop")
	return nil
}
func handleBackupRestoreSections(app *kingpin.Application, serviceName string) {
	cmd := &BackupRestoreHandler{}
	planCmd := &cli.PlanHandler{}

	backup := app.Command("backup", fmt.Sprintf("Backup %s cluster data", serviceName))
	backupStart := backup.Command(
		"start",
		"Perform cluster backup via snapshot mechanism").Action(cmd.runBackup)
	backupStart.Flag("backup_name", "Name of the snapshot").StringVar(&cmd.backupName)
	backupStart.Flag("external_location", "External location where the snapshot should be stored").StringVar(&cmd.externalLocation)
	backupStart.Flag("s3_access_key", "S3 access key").StringVar(&cmd.s3AccessKey)
	backupStart.Flag("s3_secret_key", "S3 secret key").StringVar(&cmd.s3SecretKey)
	backupStart.Flag("azure_account", "Azure storage account").StringVar(&cmd.azureAccount)
	backupStart.Flag("azure_key", "Azure secret key").StringVar(&cmd.azureKey)
	backup.Command(
		"stop",
		"Stops a currently running backup").Action(cmd.runBackupStop)
	// same as 'plan show':
	backup.Command(
		"status",
		"Displays the status of the backup").Action(planCmd.RunShow)

	restore := app.Command("restore", fmt.Sprintf("Restore %s cluster from backup", serviceName))
	restoreStart := restore.Command(
		"start",
		"Restores cluster to a previous snapshot").Action(cmd.runRestore)
	restoreStart.Flag("backup_name", "Name of the snapshot to restore").StringVar(&cmd.backupName)
	restoreStart.Flag("external_location", "External location where the snapshot is stored").StringVar(&cmd.externalLocation)
	restoreStart.Flag("s3_access_key", "S3 access key").StringVar(&cmd.s3AccessKey)
	restoreStart.Flag("s3_secret_key", "S3 secret key").StringVar(&cmd.s3SecretKey)
	restoreStart.Flag("azure_account", "Azure storage account").StringVar(&cmd.azureAccount)
	restoreStart.Flag("azure_key", "Azure secret key").StringVar(&cmd.azureKey)
	restore.Command(
		"stop",
		"Stops a currently running restore").Action(cmd.runRestoreStop)
	// same as 'plan show':
	restore.Command(
		"status",
		"Displays the status of the restore").Action(planCmd.RunShow)
}

// Reuse same struct for both 'cleanup start' and 'repair start' (same args)
type CleanupRepairHandler struct {
	nodes string
	keySpaces string
	columnFamilies string
}
func (cmd *CleanupRepairHandler) getArgs() map[string]interface{} {
	nodesList := []string{}
	nodesSplit := strings.Split(cmd.nodes, ",")
	if sliceContains(nodesSplit, "*") {
		nodesList = append(nodesList, "*")
	} else {
		for _, node := range nodesSplit {
			nodesList = append(nodesList, fmt.Sprintf("node-%s", strings.TrimSpace(node)))
		}
	}

	dict := map[string]interface{} {"nodes": nodesList}

	if len(cmd.keySpaces) != 0 {
		dict["key_spaces"] = cmd.keySpaces
	}
	if len(cmd.keySpaces) != 0 {
		dict["column_families"] = cmd.columnFamilies
	}
	return dict
}
func (cmd *CleanupRepairHandler) runCleanup(c *kingpin.ParseContext) error {
	payload, err := json.Marshal(cmd.getArgs())
	if err != nil {
		return err
	}
	cli.HTTPPutJSON("v1/cleanup/start", string(payload))
	return nil
}
func (cmd *CleanupRepairHandler) runCleanupStop(c *kingpin.ParseContext) error {
	cli.HTTPPut("v1/cleanup/stop")
	return nil
}
func (cmd *CleanupRepairHandler) runRepair(c *kingpin.ParseContext) error {
	payload, err := json.Marshal(cmd.getArgs())
	if err != nil {
		return err
	}
	cli.HTTPPutJSON("v1/repair/start", string(payload))
	return nil
}
func (cmd *CleanupRepairHandler) runRepairStop(c *kingpin.ParseContext) error {
	cli.HTTPPut("v1/repair/stop")
	return nil
}
func handleCleanupRepairSections(app *kingpin.Application) {
	cmd := &CleanupRepairHandler{}

	cleanup := app.Command("cleanup", "Clean up old token mappings")
	cleanupStart := cleanup.Command(
		"start",
		"Perform cluster cleanup of deleted or moved keys").Action(cmd.runCleanup)
	cleanupStart.Flag("nodes", "A list of the nodes to cleanup or * for all.").Default("*").StringVar(&cmd.nodes)
	cleanupStart.Flag("key_spaces", "The key spaces to cleanup or empty for all.").StringVar(&cmd.keySpaces)
	cleanupStart.Flag("column_families", "The column families to cleanup.").StringVar(&cmd.columnFamilies)
	cleanup.Command(
		"stop",
		"Stops a currently running cleanup").Action(cmd.runCleanupStop)

	repair := app.Command("repair", "Perform primary range repair")
	repairStart := repair.Command(
		"start",
		"Perform primary range anti-entropy repair").Action(cmd.runRepair)
	repairStart.Flag("nodes", "A list of the nodes to repair or * for all.").Default("*").StringVar(&cmd.nodes)
	repairStart.Flag("key_spaces", "The key spaces to repair or empty for all.").StringVar(&cmd.keySpaces)
	repairStart.Flag("column_families", "The column families to repair.").StringVar(&cmd.columnFamilies)
	repair.Command(
		"stop",
		"Stops a currently running repair").Action(cmd.runRepairStop)
}
func sliceContains(stringSlice []string, searchString string) bool {
	for _, value := range stringSlice {
		if value == searchString {
			return true
		}
	}
	return false
}
