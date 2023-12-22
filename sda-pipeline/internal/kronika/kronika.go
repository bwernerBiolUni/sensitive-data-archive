package kronika

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	b64 "encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
)

type ApiClient struct {
	client *http.Client
	config *ApiConfig
}

type ApiConfig struct {
	CaCertPath                string
	CertPath                  string
	KeyPath                   string
	ApiAddress                string
	DataSourceId              string
	StatusCheckDelayInSeconds int
	MaxNoStatusChecks         int
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

type MigrationStatus string

const (
	WaitingForStart       MigrationStatus = "0"
	UploadInProgress      MigrationStatus = "1"
	ExtractionInProgress  MigrationStatus = "2"
	StoppedByUser         MigrationStatus = "3"
	StoppedByError        MigrationStatus = "4"
	EndedWithError        MigrationStatus = "5"
	Ended                 MigrationStatus = "6"
	ErrorOccurred         MigrationStatus = "7"
	BackupMigrationStatus MigrationStatus = "8"
	NotStartedByError     MigrationStatus = "9"
)

type MigrationStatusResponse struct {
	Id             string          `json:"migration_id"`
	DataSourceName string          `json:"datasource_name"` // most likely not needed
	Status         MigrationStatus `json:"migration_status"`
	StartTime      string          `json:"migration_start_time"` // most likely not needed
}

// PendingMigrationStatuses is meant to be constant!
var PendingMigrationStatuses = []MigrationStatus{WaitingForStart, UploadInProgress, ExtractionInProgress}

// ErrorMigrationStatuses is meant to be constant!
var ErrorMigrationStatuses = []MigrationStatus{StoppedByError, EndedWithError, ErrorOccurred, NotStartedByError}

func NewApiClient(config ApiConfig) (*ApiClient, error) {
	if config.DataSourceId == "" {
		return nil, errors.New("empty dataSourceId")
	}

	if strings.Contains(config.ApiAddress, "https://") {
		tlsConfig, err := TLSConfigKronika(config)
		if err != nil {
			return nil, err
		}
		transport := &http.Transport{TLSClientConfig: tlsConfig}
		return &ApiClient{&http.Client{Transport: transport}, &config}, nil
	} else if strings.Contains(config.ApiAddress, "http://") {
		return &ApiClient{&http.Client{}, &config}, nil
	} else {
		return nil, errors.New("wrong url: " + config.ApiAddress)
	}
}

func TLSConfigKronika(config ApiConfig) (*tls.Config, error) {
	// Read system CAs
	systemCAs, err := x509.SystemCertPool()
	if err != nil {
		return nil, err
	}

	tlsConfig := tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    systemCAs,
	}

	// Add CAs for kronika
	for _, cacert := range []string{config.CaCertPath} {
		if cacert == "" {
			continue
		}
		cacert, err := os.ReadFile(cacert) // #nosec this file comes from our config
		if err != nil {
			return nil, err
		}
		if ok := tlsConfig.RootCAs.AppendCertsFromPEM(cacert); !ok {
			log.Warnln("No certs appended, using system certs only")
		}
	}

	if config.CertPath != "" && config.KeyPath != "" {
		cert, err := os.ReadFile(config.CertPath)
		if err != nil {
			return nil, err
		}
		key, err := os.ReadFile(config.KeyPath)
		if err != nil {
			return nil, err
		}
		certs, err := tls.X509KeyPair(cert, key)
		if err != nil {
			return nil, err
		}
		tlsConfig.Certificates = append(tlsConfig.Certificates, certs)
	} else {
		log.Fatalf("No certificates supplied")
	}

	return &tlsConfig, nil
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

func (api *ApiClient) CreateTemporaryLocationForFiles(input *MigrationResponse, fileName string, contentLength int64, headerSize int64) (*NewLocationResponse, error) {
	path := fmt.Sprintf("%s/ingest/migrations/%s/files", api.config.ApiAddress, input.Id)

	req, err := http.NewRequest("POST", path, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Tus-resumable", "1.0.0")
	req.Header.Set("Upload-Length", strconv.FormatInt(contentLength-headerSize, 10))
	req.Header.Set("Upload-Metadata", fmt.Sprintf("filename %s,content-type %s", b64.StdEncoding.EncodeToString([]byte(fileName)), b64.StdEncoding.EncodeToString([]byte("application/octet-stream"))))

	result, err := api.performHttpRequest(req)
	if err != nil {
		return nil, err
	}

	return &NewLocationResponse{result.header.Get("Location")}, nil
}

func (api *ApiClient) UploadDataToLocation(reader io.Reader, location string, migrationId string, uploadOffset int64) ([]byte, error) {
	locationPath := strings.Split(location, "/")

	path := fmt.Sprintf("%s/ingest/migrations/%s/files/%s", api.config.ApiAddress, migrationId, locationPath[len(locationPath)-1])

	req, err := http.NewRequest("PATCH", path, reader)
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

func (api *ApiClient) GetMigrationStatus(migrationId string) (*MigrationStatusResponse, error) {
	path := fmt.Sprintf("%s/ingest/migrations/%s", api.config.ApiAddress, migrationId)
	req, err := http.NewRequest("GET", path, nil)
	req.Header.Set("Content-Type", "application/json")

	var response MigrationStatusResponse
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
