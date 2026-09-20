package app

import (
	"context"
	"log"
	"os"
	"rag-go/chat"
	"rag-go/config"
	"rag-go/ingest"
	"rag-go/llm"
	"rag-go/vector"
	"rag-go/vector/pgvector"
	"sync"
)

func Run(parent context.Context, cfg config.Config) error {
	logger := log.New(os.Stderr, "[rag] ", log.LstdFlags)

	ctx, cancel := context.WithCancel(parent)
	defer cancel()

	client := llm.New(cfg)

	embedder := llm.NewEmbedder(cfg)

	store, err := openStore(ctx, cfg)
	if err != nil {
		logger.Printf("vector store disabled: %v", err)
	}

	var wg sync.WaitGroup
	if store != nil {
		wg.Go(func() {
			opts := ingest.Options{
				SourceDir:    cfg.IngestDir,
				ProcessedDir: cfg.ProcessedDir,
			}

			if err := ingest.Watch(ctx, opts, embedder, store, logger); err != nil && ctx.Err() == nil {
				logger.Printf("logger stopped: %v", err)
			}
		})

		logger.Printf("watching %s for new documents", cfg.IngestDir)
	}

	if store != nil {
		defer store.Close()
		logger.Println("vector store ready")
	}

	replErr := chat.RunREPL(ctx, client, chat.Options{
		SystemPromptFile: cfg.SystemPromptFile,
	})

	cancel()
	wg.Wait()
	return replErr
}

func openStore(ctx context.Context, cfg config.Config) (vector.Store, error) {
	if cfg.DatabaseURL == "" {
		return nil, nil
	}
	s, err := pgvector.New(ctx, pgvector.Options{
		DSN:          cfg.DatabaseURL,
		EmbeddingDim: cfg.EmbeddingDim,
	})
	if err != nil {
		return nil, err
	}

	return s, nil
}
