package app

import (
	"context"
	"rag-go/chat"
	"rag-go/config"
	"rag-go/llm"
)

func Run(ctx context.Context, cfg config.Config) error {
	client := llm.New(cfg)
	return chat.RunREPL(ctx, client, chat.Options{
		SystemPromptFile: cfg.SystemPromptFile,
	})
}
