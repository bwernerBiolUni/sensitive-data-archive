package kronika

import (
	"bytes"
	b64 "encoding/base64"
	"encoding/json"
	"fmt"
	log "github.com/sirupsen/logrus"
	"io"
	"net/http"
	"strconv"
	"strings"
)

type ApiClient struct {
	client *http.Client
	config ApiConfig
}

type ApiConfig struct {
	CertPath     string
	KeyPath      string
	ApiAddress   string
	DataSourceId string
}

type ApiResponse struct {
	header *http.Header
	body   []byte
}
type MigrationResponse struct {
	Id string `json:"migration_id"`
}

type NewLocationResponse struct {
	Location string `json:"Location"`
}

func NewApiClient() *ApiClient {
	return &ApiClient{&http.Client{}, ApiConfig{"test", "test", "http://localhost:8081", "12345"}}
}

func (api *ApiClient) CreateMigrationResource() (*MigrationResponse, error) {
	bodyParams := fmt.Sprintf(`{"data_source":%s}`, api.config.DataSourceId)
	path := fmt.Sprintf("%s/ingest/migrations", api.config.ApiAddress)
	req, err := http.NewRequest("POST", path, bytes.NewBuffer([]byte(bodyParams)))

	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")

	var response MigrationResponse
	result, err := api.performHttpRequest(req)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(result.body, &response)
	if err != nil {
		return nil, err
	}

	return &response, nil
}

func (api *ApiClient) CreateTemporaryLocationForFiles(input *MigrationResponse, fileName string, contentLength int64) (*NewLocationResponse, error) {
	path := fmt.Sprintf("%s/ingest/migrations/%s/files", api.config.ApiAddress, input.Id)

	req, err := http.NewRequest("POST", path, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Tus-resumable", "1.0.0")
	req.Header.Set("Upload-Length", strconv.FormatInt(contentLength, 10))
	req.Header.Set("Upload-Metadata", fmt.Sprintf("filename %s,content-type %s", b64.StdEncoding.EncodeToString([]byte(fileName)), b64.StdEncoding.EncodeToString([]byte("application/octet-stream"))))

	result, err := api.performHttpRequest(req)
	if err != nil {
		return nil, err
	}

	return &NewLocationResponse{result.header.Get("Location")}, nil
}

func (api *ApiClient) UploadDataToLocation(buffer []byte, location string, migrationId string, offset int64, buffSize int) ([]byte, error) {
	locationPath := strings.Split(location, "/")

	uploadOffset := offset - int64(buffSize)

	if uploadOffset < 0 {
		uploadOffset = 0
	}

	path := fmt.Sprintf("%s/ingest/migrations/%s/files/%s", api.config.ApiAddress, migrationId, locationPath[len(locationPath)-1])
	chunk := bytes.NewReader(buffer)

	req, err := http.NewRequest("PATCH", path, chunk)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/offset+octet-stream")
	req.Header.Set("Tus-resumable", "1.0.0")
	req.Header.Set("Upload-Offset", strconv.FormatInt(uploadOffset, 10))

	result, err := api.performHttpRequest(req)
	if err != nil {
		return nil, err
	}

	return result.body, nil
}

func (api *ApiClient) StartMigration(input *MigrationResponse) (*ApiResponse, error) {
	path := fmt.Sprintf("%s/ingest/migrations/%s/start", api.config.ApiAddress, input.Id)
	req, err := http.NewRequest("GET", path, nil)
	req.Header.Set("Content-Type", "application/json")

	if err != nil {
		return nil, err
	}

	return api.performHttpRequest(req)
}

func (api *ApiClient) performHttpRequest(req *http.Request) (*ApiResponse, error) {
	log.Infof("[Kronik@] Sending %s to %s - %s", req.Method, req.URL, req.Header)
	resp, err := api.client.Do(req)

	if err != nil {
		return nil, err
	}

	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			log.Error(err)
		}
	}(resp.Body)

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	return &ApiResponse{&resp.Header, data}, nil
}
