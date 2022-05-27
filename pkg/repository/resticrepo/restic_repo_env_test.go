/*
Copyright the Velero contributors.

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

package resticrepo

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	velerotest "github.com/vmware-tanzu/velero/pkg/test"
)

func TestTempCACertFile(t *testing.T) {
	var (
		fs         = velerotest.NewFakeFileSystem()
		caCertData = []byte("cacert")
	)

	fileName, err := TempCACertFile(caCertData, "default", fs)
	require.NoError(t, err)

	contents, err := fs.ReadFile(fileName)
	require.NoError(t, err)

	assert.Equal(t, string(caCertData), string(contents))

	os.Remove(fileName)
}
