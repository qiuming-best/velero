/*
Copyright The Velero Contributors.

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

package kopiaup

import (
	"sync/atomic"
	"time"

	"github.com/vmware-tanzu/velero/pkg/uploader/upimpl"
)

type Throttle struct {
	throttle int64
	interval time.Duration
}

func (t *Throttle) ShouldOutput() bool {
	nextOutputTimeUnixNano := atomic.LoadInt64(&t.throttle)
	if nowNano := time.Now().UnixNano(); nowNano > nextOutputTimeUnixNano { //nolint:forbidigo
		if atomic.CompareAndSwapInt64(&t.throttle, nextOutputTimeUnixNano, nowNano+t.interval.Nanoseconds()) {
			return true
		}
	}
	return false
}

func (p *KopiaProgress) InitThrottle(interval time.Duration) {
	p.outputThrottle.throttle = 0
	p.outputThrottle.interval = interval
}

type KopiaProgress struct {
	// all int64 must precede all int32 due to alignment requirements on ARM
	// +checkatomic
	uploadedBytes int64
	cachedBytes   int64
	hashededBytes int64
	// +checkatomic
	uploadedFiles int32
	// +checkatomic
	ignoredErrorCount int32
	// +checkatomic
	fatalErrorCount     int32
	estimatedFileCount  int32 // +checklocksignore
	estimatedTotalBytes int64 // +checklocksignore
	// +checkatomic
	processedBytes int64
	outputThrottle Throttle // is int64
	UpFunc         func(upimpl.UploaderProgress)
}

func (p *KopiaProgress) UploadedBytes(numBytes int64) {
	atomic.AddInt64(&p.uploadedBytes, numBytes)
	atomic.AddInt32(&p.uploadedFiles, 1)

	p.UpdateProgress()
}

func (p *KopiaProgress) Error(path string, err error, isIgnored bool) {
	if isIgnored {
		atomic.AddInt32(&p.ignoredErrorCount, 1)
	} else {
		atomic.AddInt32(&p.fatalErrorCount, 1)
	}
}

func (p *KopiaProgress) EstimatedDataSize(fileCount int, totalBytes int64) {
	atomic.StoreInt64(&p.estimatedTotalBytes, totalBytes)
	atomic.StoreInt32(&p.estimatedFileCount, int32(fileCount))

	p.UpdateProgress()
}

func (p *KopiaProgress) UpdateProgress() {
	if p.outputThrottle.ShouldOutput() {
		p.UpFunc(upimpl.UploaderProgress{
			TotalBytes: atomic.LoadInt64(&p.estimatedTotalBytes),
			BytesDone:  atomic.LoadInt64(&p.processedBytes),
		})
	}
}

func (p *KopiaProgress) UploadStarted() {}

func (p *KopiaProgress) CachedFile(fname string, numBytes int64) {
	atomic.AddInt64(&p.cachedBytes, numBytes)
	p.UpdateProgress()
}

func (p *KopiaProgress) HashedBytes(numBytes int64) {
	atomic.AddInt64(&p.processedBytes, numBytes)
	atomic.AddInt64(&p.hashededBytes, numBytes)
	p.UpdateProgress()
}

func (p *KopiaProgress) HashingFile(fname string) {}

func (p *KopiaProgress) ExcludedFile(fname string, numBytes int64) {}

func (p *KopiaProgress) ExcludedDir(dirname string) {}

func (p *KopiaProgress) FinishedHashingFile(fname string, numBytes int64) {
	p.UpdateProgress()
}

func (p *KopiaProgress) StartedDirectory(dirname string) {}

func (p *KopiaProgress) FinishedDirectory(dirname string) {
	p.UpdateProgress()
}

func (p *KopiaProgress) UploadFinished() {}

func (p *KopiaProgress) ProgressBytes(processedBytes int64, totalBytes int64) {
	atomic.AddInt64(&p.processedBytes, processedBytes)
	atomic.AddInt64(&p.estimatedTotalBytes, totalBytes)
	p.UpdateProgress()
}
