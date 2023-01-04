/*
Copyright 2020 the Velero contributors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package framework

import (
	"fmt"
	"os"
	"strings"

	plugin "github.com/hashicorp/go-plugin"
	"github.com/sirupsen/logrus"
	"github.com/spf13/pflag"

	"github.com/vmware-tanzu/velero/pkg/plugin/framework/common"
	"github.com/vmware-tanzu/velero/pkg/util/logging"
)

// Server serves registered plugin implementations.
type Server interface {
	// BindFlags defines the plugin server's command-line flags
	// on the provided FlagSet. If you're not sure what flag set
	// to use, pflag.CommandLine is the default set of command-line
	// flags.
	//
	// This method must be called prior to calling .Serve().
	BindFlags(flags *pflag.FlagSet) Server

	// RegisterBackupItemAction registers a backup item action. Accepted format
	// for the plugin name is <DNS subdomain>/<non-empty name>.
	RegisterBackupItemAction(pluginName string, initializer common.HandlerInitializer) Server

	// RegisterBackupItemActions registers multiple backup item actions.
	RegisterBackupItemActions(map[string]common.HandlerInitializer) Server

	// RegisterVolumeSnapshotter registers a volume snapshotter. Accepted format
	// for the plugin name is <DNS subdomain>/<non-empty name>.
	RegisterVolumeSnapshotter(pluginName string, initializer common.HandlerInitializer) Server

	// RegisterVolumeSnapshotters registers multiple volume snapshotters.
	RegisterVolumeSnapshotters(map[string]common.HandlerInitializer) Server

	// RegisterObjectStore registers an object store. Accepted format
	// for the plugin name is <DNS subdomain>/<non-empty name>.
	RegisterObjectStore(pluginName string, initializer common.HandlerInitializer) Server

	// RegisterObjectStores registers multiple object stores.
	RegisterObjectStores(map[string]common.HandlerInitializer) Server

	// RegisterRestoreItemAction registers a restore item action. Accepted format
	// for the plugin name is <DNS subdomain>/<non-empty name>.
	RegisterRestoreItemAction(pluginName string, initializer common.HandlerInitializer) Server

	// RegisterRestoreItemActions registers multiple restore item actions.
	RegisterRestoreItemActions(map[string]common.HandlerInitializer) Server

	// RegisterDeleteItemAction registers a delete item action. Accepted format
	// for the plugin name is <DNS subdomain>/<non-empty name>.
	RegisterDeleteItemAction(pluginName string, initializer common.HandlerInitializer) Server

	// RegisterDeleteItemActions registers multiple Delete item actions.
	RegisterDeleteItemActions(map[string]common.HandlerInitializer) Server

	RegisterItemSnapshotter(pluginName string, initializer common.HandlerInitializer) Server

	// RegisterItemSnapshotters registers multiple Item Snapshotters
	RegisterItemSnapshotters(map[string]common.HandlerInitializer) Server

	// Server runs the plugin server.
	Serve()
}

// server implements Server.
type server struct {
	log               *logrus.Logger
	logLevelFlag      *logging.LevelFlag
	flagSet           *pflag.FlagSet
	backupItemAction  *BackupItemActionPlugin
	volumeSnapshotter *VolumeSnapshotterPlugin
	objectStore       *ObjectStorePlugin
	restoreItemAction *RestoreItemActionPlugin
	deleteItemAction  *DeleteItemActionPlugin
	itemSnapshotter   *ItemSnapshotterPlugin
}

// NewServer returns a new Server
func NewServer() Server {
	log := newLogger()

	return &server{
		log:               log,
		logLevelFlag:      logging.LogLevelFlag(log.Level),
		backupItemAction:  NewBackupItemActionPlugin(common.ServerLogger(log)),
		volumeSnapshotter: NewVolumeSnapshotterPlugin(common.ServerLogger(log)),
		objectStore:       NewObjectStorePlugin(common.ServerLogger(log)),
		restoreItemAction: NewRestoreItemActionPlugin(common.ServerLogger(log)),
		deleteItemAction:  NewDeleteItemActionPlugin(common.ServerLogger(log)),
		itemSnapshotter:   NewItemSnapshotterPlugin(common.ServerLogger(log)),
	}
}

func (s *server) BindFlags(flags *pflag.FlagSet) Server {
	flags.Var(s.logLevelFlag, "log-level", fmt.Sprintf("The level at which to log. Valid values are %s.", strings.Join(s.logLevelFlag.AllowedValues(), ", ")))
	s.flagSet = flags
	s.flagSet.ParseErrorsWhitelist.UnknownFlags = true

	return s
}

func (s *server) RegisterBackupItemAction(name string, initializer common.HandlerInitializer) Server {
	s.backupItemAction.Register(name, initializer)
	return s
}

func (s *server) RegisterBackupItemActions(m map[string]common.HandlerInitializer) Server {
	for name := range m {
		s.RegisterBackupItemAction(name, m[name])
	}
	return s
}

func (s *server) RegisterVolumeSnapshotter(name string, initializer common.HandlerInitializer) Server {
	s.volumeSnapshotter.Register(name, initializer)
	return s
}

func (s *server) RegisterVolumeSnapshotters(m map[string]common.HandlerInitializer) Server {
	for name := range m {
		s.RegisterVolumeSnapshotter(name, m[name])
	}
	return s
}

func (s *server) RegisterObjectStore(name string, initializer common.HandlerInitializer) Server {
	s.objectStore.Register(name, initializer)
	return s
}

func (s *server) RegisterObjectStores(m map[string]common.HandlerInitializer) Server {
	for name := range m {
		s.RegisterObjectStore(name, m[name])
	}
	return s
}

func (s *server) RegisterRestoreItemAction(name string, initializer common.HandlerInitializer) Server {
	s.restoreItemAction.Register(name, initializer)
	return s
}

func (s *server) RegisterRestoreItemActions(m map[string]common.HandlerInitializer) Server {
	for name := range m {
		s.RegisterRestoreItemAction(name, m[name])
	}
	return s
}

func (s *server) RegisterDeleteItemAction(name string, initializer common.HandlerInitializer) Server {
	s.deleteItemAction.Register(name, initializer)
	return s
}

func (s *server) RegisterDeleteItemActions(m map[string]common.HandlerInitializer) Server {
	for name := range m {
		s.RegisterDeleteItemAction(name, m[name])
	}
	return s
}

func (s *server) RegisterItemSnapshotter(name string, initializer common.HandlerInitializer) Server {
	s.itemSnapshotter.Register(name, initializer)
	return s
}
func (s *server) RegisterItemSnapshotters(m map[string]common.HandlerInitializer) Server {
	for name := range m {
		s.RegisterItemSnapshotter(name, m[name])
	}
	return s
}

// getNames returns a list of PluginIdentifiers registered with plugin.
func getNames(command string, kind common.PluginKind, plugin Interface) []PluginIdentifier {
	var pluginIdentifiers []PluginIdentifier

	for _, name := range plugin.Names() {
		id := PluginIdentifier{Command: command, Kind: kind, Name: name}
		pluginIdentifiers = append(pluginIdentifiers, id)
	}

	return pluginIdentifiers
}

func (s *server) Serve() {
	if s.flagSet != nil && !s.flagSet.Parsed() {
		s.log.Debugf("Parsing flags")
		s.flagSet.Parse(os.Args[1:])
	}

	s.log.Level = s.logLevelFlag.Parse()
	s.log.Debugf("Setting log level to %s", strings.ToUpper(s.log.Level.String()))

	s.log.Info("Start Serve")
	defer s.log.Info("Stop Serve")

	command := os.Args[0]

	var pluginIdentifiers []PluginIdentifier
	pluginIdentifiers = append(pluginIdentifiers, getNames(command, common.PluginKindBackupItemAction, s.backupItemAction)...)
	pluginIdentifiers = append(pluginIdentifiers, getNames(command, common.PluginKindVolumeSnapshotter, s.volumeSnapshotter)...)
	pluginIdentifiers = append(pluginIdentifiers, getNames(command, common.PluginKindObjectStore, s.objectStore)...)
	pluginIdentifiers = append(pluginIdentifiers, getNames(command, common.PluginKindRestoreItemAction, s.restoreItemAction)...)
	pluginIdentifiers = append(pluginIdentifiers, getNames(command, common.PluginKindDeleteItemAction, s.deleteItemAction)...)
	pluginIdentifiers = append(pluginIdentifiers, getNames(command, common.PluginKindItemSnapshotter, s.itemSnapshotter)...)

	pluginLister := NewPluginLister(pluginIdentifiers...)

	plugin.Serve(&plugin.ServeConfig{
		HandshakeConfig: Handshake(),
		Plugins: map[string]plugin.Plugin{
			string(common.PluginKindBackupItemAction):  s.backupItemAction,
			string(common.PluginKindVolumeSnapshotter): s.volumeSnapshotter,
			string(common.PluginKindObjectStore):       s.objectStore,
			string(common.PluginKindPluginLister):      NewPluginListerPlugin(pluginLister),
			string(common.PluginKindRestoreItemAction): s.restoreItemAction,
			string(common.PluginKindDeleteItemAction):  s.deleteItemAction,
			string(common.PluginKindItemSnapshotter):   s.itemSnapshotter,
		},
		GRPCServer: plugin.DefaultGRPCServer,
	})
}
