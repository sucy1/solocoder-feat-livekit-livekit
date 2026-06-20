// Copyright 2023 LiveKit, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package service

import (
	"context"
	"time"

	"github.com/livekit/livekit-server/pkg/config"
)

const (
	DefaultRPCTimeout = 10 * time.Second
	MaxRPCTimeout     = 60 * time.Second
)

func ClampRPCTimeout(timeout time.Duration, conf config.RPCTimeoutConfig) time.Duration {
	defaultTimeout := conf.DefaultTimeout
	if defaultTimeout == 0 {
		defaultTimeout = DefaultRPCTimeout
	}
	maxTimeout := conf.MaxTimeout
	if maxTimeout == 0 {
		maxTimeout = MaxRPCTimeout
	}

	if timeout == 0 {
		timeout = defaultTimeout
	}
	if timeout > maxTimeout {
		timeout = maxTimeout
	}
	return timeout
}

func WithRPCTimeout(ctx context.Context, timeout time.Duration, conf config.RPCTimeoutConfig) (context.Context, context.CancelFunc) {
	clamped := ClampRPCTimeout(timeout, conf)
	return context.WithTimeout(ctx, clamped)
}
