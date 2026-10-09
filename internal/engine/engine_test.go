// SPDX-FileCopyrightText: 2026 Scitrera LLC
// SPDX-License-Identifier: Apache-2.0
package engine

import (
	"context"
	"io"
	"strings"
	"testing"
)

func TestProgressDetectsAsynchronousDaemonFailure(t *testing.T) {
	for _, input := range []string{
		"{\"status\":\"started\"}\n{\"error\":\"digest mismatch\"}\n",
		"{\"errorDetail\":{\"message\":\"disk full\"}}\n",
		"not json\n",
		strings.Repeat("x", 2<<20),
	} {
		if err := progress(context.Background(), io.NopCloser(strings.NewReader(input))); err == nil {
			t.Fatalf("accepted failed progress stream")
		}
	}
	if err := progress(context.Background(), io.NopCloser(strings.NewReader("{\"status\":\"complete\"}\n"))); err != nil {
		t.Fatal(err)
	}
}

func TestProgressObservation(t *testing.T) {
	var seen []Progress
	input := "{\"id\":\"layer-a\",\"status\":\"Download complete\"}\n{\"id\":\"layer-a\",\"status\":\"Pull complete\"}\n"
	err := progress(context.Background(), io.NopCloser(strings.NewReader(input)), func(p Progress) { seen = append(seen, p) })
	if err != nil || len(seen) != 2 || seen[0].ID != "layer-a" || seen[1].Status != "Pull complete" {
		t.Fatalf("observation: %v %v", seen, err)
	}
}

func TestDockerExtractionCounters(t *testing.T) {
	input := `{"id":"abcdef123456","status":"Extracting","progressDetail":{"current":1073741824,"total":2147483648}}`
	var got Progress
	err := progress(context.Background(), io.NopCloser(strings.NewReader(input)), func(p Progress) { got = p })
	if err != nil || got.ProgressDetail.Current != 1<<30 || got.ProgressDetail.Total != 2<<30 {
		t.Fatalf("lost extraction detail: %+v %v", got, err)
	}
}
