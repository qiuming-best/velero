package kopiaup

import (
	"fmt"
	"sync/atomic"

	"github.com/vmware-tanzu/velero/pkg/uploader/upimpl"
)

type KopiaProgress struct {
	// all int64 must precede all int32 due to alignment requirements on ARM
	// +checkatomic
	uploadedBytes int64

	// +checkatomic
	uploadedFiles int32

	// +checkatomic
	ignoredErrorCount int32

	// +checkatomic
	fatalErrorCount int32

	estimatedFileCount int32 // +checklocksignore

	estimatedTotalBytes int64 // +checklocksignore

	totalCachedFiles int32

	totalHashedFiles int32

	UpFunc func(upimpl.UploaderProgress)
}

func (p *KopiaProgress) UploadedBytes(numBytes int64) {
	atomic.AddInt64(&p.uploadedBytes, numBytes)
	p.ShowProgress("UploadedBytes")
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

	p.ShowProgress("EstimatedDataSize")
}

func (p *KopiaProgress) ShowProgress(msg string) {
	p.UpFunc(upimpl.UploaderProgress{
		TotalBytes: atomic.LoadInt64(&p.estimatedTotalBytes),
		BytesDone:  atomic.LoadInt64(&p.uploadedBytes),
		Msg:        fmt.Sprintf("mgs %v %v %v", msg, p.estimatedTotalBytes, p.uploadedBytes),
	})
}

func (p *KopiaProgress) UploadStarted() {
	p.ShowProgress("UploadStarted")
}

func (p *KopiaProgress) CachedFile(fname string, numBytes int64) {
	atomic.AddInt32(&p.totalCachedFiles, 1)
	//atomic.AddInt64(&p.uploadedBytes, numBytes)
	p.ShowProgress("CachedFile")
}

func (p *KopiaProgress) HashedBytes(numBytes int64) {
	//atomic.AddInt64(&p.uploadedBytes, numBytes)
	p.ShowProgress("HashedBytes")
}

func (p *KopiaProgress) HashingFile(fname string) { p.ShowProgress("HashingFile") }

func (p *KopiaProgress) ExcludedFile(fname string, numBytes int64) { p.ShowProgress("ExcludedFile") }

func (p *KopiaProgress) ExcludedDir(dirname string) { p.ShowProgress("ExcludedDir") }

func (p *KopiaProgress) FinishedHashingFile(fname string, numBytes int64) {
	//atomic.AddInt32(&p.totalHashedFiles, 1)
	p.ShowProgress("FinishedHashingFile")
}

func (p *KopiaProgress) StartedDirectory(dirname string) { p.ShowProgress("StartedDirectory") }

func (p *KopiaProgress) FinishedDirectory(dirname string) { p.ShowProgress("FinishedDirectory") }

func (p *KopiaProgress) UploadFinished() { p.ShowProgress("UploadFinished") }
