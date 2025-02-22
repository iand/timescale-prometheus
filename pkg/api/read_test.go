package api

import (
	"fmt"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/timescale/timescale-prometheus/pkg/prompb"
	"net/http"
	"testing"
)

func TestRead(t *testing.T) {
	testCases := []struct {
		name               string
		responseCode       int
		requestBody        string
		readerResponse     *prompb.ReadResponse
		readerErr          error
		expReceivedQueries float64
	}{
		{
			name:         "read request body error",
			responseCode: http.StatusBadRequest,
		},
		{
			name:         "malformed compression data",
			responseCode: http.StatusBadRequest,
			requestBody:  "123",
		},
		{
			name:         "malformed read request",
			responseCode: http.StatusBadRequest,
			requestBody:  string(snappy.Encode(nil, []byte("test"))),
		},
		{
			name:         "reader error",
			responseCode: http.StatusInternalServerError,
			readerErr:    fmt.Errorf("some error"),
			requestBody: readRequestToString(
				&prompb.ReadRequest{Queries: []*prompb.Query{{}}},
			),
			expReceivedQueries: 1,
		},
		{
			name:           "happy path",
			responseCode:   http.StatusOK,
			readerResponse: &prompb.ReadResponse{},
			requestBody: readRequestToString(
				&prompb.ReadRequest{},
			),
		}, {
			name:           "happy path with query",
			responseCode:   http.StatusOK,
			readerResponse: &prompb.ReadResponse{},
			requestBody: readRequestToString(
				&prompb.ReadRequest{Queries: []*prompb.Query{{}}},
			),
			expReceivedQueries: 1,
		},
	}

	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			mockReader := &mockReader{
				response: c.readerResponse,
				err:      c.readerErr,
			}
			receivedQueriesCounter := &mockMetric{}
			queryDurationHist := &mockMetric{}
			failedQueriesCounter := &mockMetric{}
			metrics := &Metrics{
				QueryBatchDuration: queryDurationHist,
				FailedQueries:      failedQueriesCounter,
				ReceivedQueries:    receivedQueriesCounter,
			}
			handler := Read(mockReader, metrics)

			test := GenerateHandleTester(t, handler)

			w := test("GET", getReader(c.requestBody))

			if w.Code != c.responseCode {
				t.Errorf("Unexpected HTTP status code received: got %d wanted %d", w.Code, c.responseCode)
			}

			if receivedQueriesCounter.value != c.expReceivedQueries {
				t.Errorf("expected %f queries to be received, got %f", c.expReceivedQueries, receivedQueriesCounter.value)
			}
			if c.responseCode == http.StatusOK && queryDurationHist.value == 0 {
				t.Error("expected recorded query duration to be > 0")
			}
			if c.responseCode == http.StatusInternalServerError && failedQueriesCounter.value == 0 {
				t.Error("expected number of failed queries to be > 0")
			}
		})
	}
}

func readRequestToString(r *prompb.ReadRequest) string {
	data, _ := proto.Marshal(r)
	return string(snappy.Encode(nil, data))
}

type mockReader struct {
	request  *prompb.ReadRequest
	response *prompb.ReadResponse
	err      error
}

func (m *mockReader) Read(r *prompb.ReadRequest) (*prompb.ReadResponse, error) {
	m.request = r
	return m.response, m.err
}
