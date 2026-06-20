package service

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"

	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"

	"github.com/livekit/livekit-server/pkg/config"
)

type TranscodingService struct {
	config config.TranscodingConfig

	queue      chan *transcodingJob
	activeJobs sync.WaitGroup
	stopChan   chan struct{}
	stopped    bool
	mu         sync.Mutex
}

type transcodingJob struct {
	EgressID   string
	InputFile  string
	OutputFile string
	RoomID     livekit.RoomID
	RoomName   livekit.RoomName
}

func NewTranscodingService(conf config.TranscodingConfig) *TranscodingService {
	if !conf.Enabled {
		return nil
	}
	if conf.MaxConcurrency <= 0 {
		conf.MaxConcurrency = 3
	}
	s := &TranscodingService{
		config:   conf,
		queue:    make(chan *transcodingJob, 100),
		stopChan: make(chan struct{}),
	}
	s.startWorkers()
	return s
}

func (s *TranscodingService) startWorkers() {
	for i := 0; i < s.config.MaxConcurrency; i++ {
		go s.worker(i)
	}
}

func (s *TranscodingService) worker(id int) {
	logger.Infow("transcoding worker started", "workerID", id)
	for {
		select {
		case <-s.stopChan:
			logger.Infow("transcoding worker stopped", "workerID", id)
			return
		case job, ok := <-s.queue:
			if !ok {
				return
			}
			s.activeJobs.Add(1)
			s.processJob(job)
			s.activeJobs.Done()
		}
	}
}

func (s *TranscodingService) processJob(job *transcodingJob) {
	logger.Infow(
		"starting transcoding job",
		"egressID", job.EgressID,
		"input", job.InputFile,
		"output", job.OutputFile,
	)

	ctx, cancel := context.WithTimeout(context.Background(), s.config.Timeout)
	defer cancel()

	if _, err := os.Stat(job.InputFile); os.IsNotExist(err) {
		logger.Errorw("input file not found", err, "egressID", job.EgressID, "input", job.InputFile)
		return
	}

	outputDir := filepath.Dir(job.OutputFile)
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		logger.Errorw("failed to create output directory", err, "egressID", job.EgressID, "dir", outputDir)
		return
	}

	args := s.buildFFmpegArgs(job.InputFile, job.OutputFile)
	cmd := exec.CommandContext(ctx, s.config.FFmpegPath, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		logger.Errorw(
			"transcoding failed, keeping original file",
			err,
			"egressID", job.EgressID,
			"input", job.InputFile,
		)
		return
	}

	logger.Infow(
		"transcoding completed successfully",
		"egressID", job.EgressID,
		"output", job.OutputFile,
	)

	if !s.config.KeepOriginal {
		if err := os.Remove(job.InputFile); err != nil {
			logger.Warnw("failed to remove original file", err, "file", job.InputFile)
		}
	}
}

func (s *TranscodingService) buildFFmpegArgs(input, output string) []string {
	args := []string{"-y", "-i", input}

	if s.config.OutputResolution != "" {
		args = append(args, "-s", s.config.OutputResolution)
	}

	args = append(args, output)
	return args
}

func (s *TranscodingService) SubmitJob(info *livekit.EgressInfo) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.stopped {
		return
	}

	inputFile := s.getOutputFile(info)
	if inputFile == "" {
		return
	}

	outputFile := s.generateOutputPath(inputFile)

	job := &transcodingJob{
		EgressID:   info.EgressId,
		InputFile:  inputFile,
		OutputFile: outputFile,
		RoomID:     livekit.RoomID(info.RoomId),
		RoomName:   livekit.RoomName(info.RoomName),
	}

	select {
	case s.queue <- job:
		logger.Infow("transcoding job queued", "egressID", info.EgressId)
	default:
		logger.Warnw("transcoding queue full, dropping job", nil, "egressID", info.EgressId)
	}
}

func (s *TranscodingService) getOutputFile(info *livekit.EgressInfo) string {
	switch result := info.Result.(type) {
	case *livekit.EgressInfo_File:
		if result.File != nil && result.File.Filename != "" {
			return result.File.Filename
		}
	case *livekit.EgressInfo_Segments:
		if result.Segments != nil && len(result.Segments.SegmentNames) > 0 {
			last := result.Segments.SegmentNames[len(result.Segments.SegmentNames)-1]
			return last
		}
	}
	return ""
}

func (s *TranscodingService) generateOutputPath(inputPath string) string {
	ext := filepath.Ext(inputPath)
	base := inputPath[:len(inputPath)-len(ext)]
	newExt := "." + s.config.OutputFormat
	if newExt == ext {
		return base + "_transcoded" + ext
	}
	return base + newExt
}

func (s *TranscodingService) Stop() {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return
	}
	s.stopped = true
	close(s.stopChan)
	close(s.queue)
	s.mu.Unlock()

	logger.Infow("waiting for active transcoding jobs to complete")
	s.activeJobs.Wait()
	logger.Infow("transcoding service stopped")
}

func (s *TranscodingService) OnEgressEnded(ctx context.Context, info *livekit.EgressInfo) {
	if info.Status != livekit.EgressStatus_EGRESS_COMPLETE && info.Status != livekit.EgressStatus_EGRESS_FAILED {
		return
	}
	if info.Status == livekit.EgressStatus_EGRESS_FAILED {
		logger.Infow("egress failed, skipping transcoding", "egressID", info.EgressId)
		return
	}
	s.SubmitJob(info)
}

func (s *TranscodingService) String() string {
	return fmt.Sprintf("TranscodingService{enabled: %v, maxConcurrency: %d, format: %s}",
		s.config.Enabled, s.config.MaxConcurrency, s.config.OutputFormat)
}
