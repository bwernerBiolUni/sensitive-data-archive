// The verify service reads and decrypts ingested files from the archive
// storage and sends accession requests.
package main

import (
	"encoding/json"
	log "github.com/sirupsen/logrus"
	"golang.org/x/exp/slices"
	"time"

	"sda-pipeline/internal/broker"
	"sda-pipeline/internal/config"
	"sda-pipeline/internal/database"
	"sda-pipeline/internal/kronika"
)

// Message struct that holds the json message data
type message struct {
	FilePath           string      `json:"filepath"`
	User               string      `json:"user"`
	FileID             string      `json:"file_id"`
	ArchivePath        string      `json:"archive_path"`
	EncryptedChecksums []checksums `json:"encrypted_checksums"`
	ReVerify           bool        `json:"re_verify"`
}

// Verified is struct holding the full message data
type verified struct {
	User               string      `json:"user"`
	FilePath           string      `json:"filepath"`
	DecryptedChecksums []checksums `json:"decrypted_checksums"`
}

// Checksums is struct for the checksum type and value
type checksums struct {
	Type  string `json:"type"`
	Value string `json:"value"`
}

func main() {
	forever := make(chan bool)
	conf, err := config.NewConfig("verify")
	if err != nil {
		log.Fatal(err)
	}
	mq, err := broker.NewMQ(conf.Broker)
	if err != nil {
		log.Fatal(err)
	}
	db, err := database.NewDB(conf.Database)
	if err != nil {
		log.Fatal(err)
	}
	kronikaClient, err := kronika.NewApiClient(conf.Kronika)
	if err != nil {
		log.Fatal(err)
	}

	defer mq.Channel.Close()
	defer mq.Connection.Close()
	defer db.Close()

	go func() {
		connError := mq.ConnectionWatcher()
		log.Error(connError)
		forever <- false
	}()

	go func() {
		connError := mq.ChannelWatcher()
		log.Error(connError)
		forever <- false
	}()

	log.Info("starting verify service")

	go func() {
		messages, err := mq.GetMessages(conf.Broker.Queue)
		if err != nil {
			log.Fatalf("Failed to get messages (error: %v) ",
				err)
		}
		for delivered := range messages {
			var message message
			log.Debugf("Received a message (corr-id: %s, message: %s)",
				delivered.CorrelationId,
				delivered.Body)

			err := mq.ValidateJSON(&delivered, "ingestion-verification", delivered.Body, &message)

			if err != nil {
				log.Errorf("Validation (ingestion-verifiation) of incoming message failed "+
					"(corr-id: %s, error: %v, message: %s)",
					delivered.CorrelationId,
					err,
					delivered.Body)

				// Restart on new message
				continue
			}

			// we unmarshal the message in the validation step so this is safe to do
			_ = json.Unmarshal(delivered.Body, &message)

			log.Infof("Received work "+
				"(corr-id: %s, user: %s, filepath: %s, fileid: %s, archivepath: %s, encryptedchecksums: %v, reverify: %t)",
				delivered.CorrelationId,
				message.User,
				message.FilePath,
				message.FileID,
				message.ArchivePath,
				message.EncryptedChecksums,
				message.ReVerify)

			// If the file has been canceled by the uploader, don't spend time working on it.
			status, err := db.GetFileStatus(delivered.CorrelationId)
			if err != nil {
				log.Errorf("failed to get file status, reason: %v", err.Error())
				// Send the message to an error queue so it can be analyzed.
				infoErrorMessage := broker.InfoError{
					Error:           "Getheader failed",
					Reason:          err.Error(),
					OriginalMessage: message,
				}

				body, _ := json.Marshal(infoErrorMessage)
				if e := mq.SendMessage(delivered.CorrelationId, conf.Broker.Exchange, conf.Broker.RoutingError, conf.Broker.Durable, body); e != nil {
					log.Errorf("failed so publish message, reason: %v", err)
				}

				if err := delivered.Ack(false); err != nil {
					log.Errorf("Failed acking canceled work, reason: %v", err)
				}

				continue
			}
			if status == "disabled" {
				log.Infof("file with correlation ID: %s is disabled, stopping verification", delivered.CorrelationId)
				if err := delivered.Ack(false); err != nil {
					log.Errorf("Failed acking canceled work, reason: %v", err)
				}

				continue
			}

			migrationId, err := db.GetMigrationId(message.FileID)
			if err != nil {
				log.Errorf("GetMigrationId failed (file-id: %s, corr-id: %s, user: %s, filepath: %s, reason: %v)",
					message.FileID, delivered.CorrelationId, message.User, message.FilePath, err)
			}

			var migrationStatusResponse *kronika.MigrationStatusResponse
			for i := 0; i < conf.Kronika.MaxNoStatusChecks; i++ {
				migrationStatusResponse, err = kronikaClient.GetMigrationStatus(migrationId)
				if err != nil {
					log.Errorf("Failed to check Kronika migration status (corr-id: %s, user: %s, filepath: %s, migrationid: %s, reason: %v)",
						delivered.CorrelationId, message.User, message.FilePath, migrationId, err)

					// Send the message to an error queue, so it can be analyzed.
					infoErrorMessage := broker.InfoError{
						Error:           "Failed to check Kronika migration status",
						Reason:          err.Error(),
						OriginalMessage: message,
					}

					body, _ := json.Marshal(infoErrorMessage)
					if e := mq.SendMessage(delivered.CorrelationId, conf.Broker.Exchange, conf.Broker.RoutingError, conf.Broker.Durable, body); e != nil {
						log.Errorf("Failed to publish error message "+
							"(corr-id: %s, user: %s, filepath: %s, archivepath: %s, encryptedchecksums: %v, reverify: %t, reason: %v)",
							delivered.CorrelationId,
							message.User,
							message.FilePath,
							message.ArchivePath,
							message.EncryptedChecksums,
							message.ReVerify,
							e)
					}
					continue
				}

				if !slices.Contains(kronika.PendingMigrationStatuses, migrationStatusResponse.Status) {
					break
				}

				time.Sleep(time.Duration(conf.Kronika.StatusCheckDelayInSeconds) * time.Second)
			}

			if !slices.Contains(kronika.ErrorMigrationStatuses, migrationStatusResponse.Status) {
				log.Errorf("Migration ended with error status: %s "+
					"(corr-id: %s, user: %s, filepath: %s, fileid: %s, migrationId: %s, encryptedchecksums: %v, reverify: %t)",
					migrationStatusResponse.Status,
					delivered.CorrelationId,
					message.User,
					message.FilePath,
					message.FileID,
					migrationId,
					message.EncryptedChecksums,
					message.ReVerify)

				if e := delivered.Nack(false, false); e != nil {
					log.Errorf("Failed to nack message "+
						"(corr-id: %s, user: %s, filepath: %s, fileid: %s, migrationid: %s, migrationStatus: %s, encryptedchecksums: %v, reverify: %t, reason: %v)",
						delivered.CorrelationId,
						message.User,
						message.FilePath,
						message.FileID,
						migrationId,
						migrationStatusResponse.Status,
						message.EncryptedChecksums,
						message.ReVerify,
						e)
				}
				continue
			}

			if e := db.MarkCompleted(message.FileID, delivered.CorrelationId); e != nil {
				log.Errorf("MarkCompleted failed "+
					"(corr-id: %s, user: %s, filepath: %s, migrationId: %s, encryptedchecksums: %v, reverify: %t, reason: %v)",
					delivered.CorrelationId,
					message.User,
					message.FilePath,
					migrationId,
					message.EncryptedChecksums,
					message.ReVerify,
					e)

				continue
				// this should really be handled by the DB retry mechanism
			}

			log.Infof("File marked completed "+
				"(corr-id: %s, user: %s, filepath: %s, migrationId: %s, encryptedchecksums: %v, reverify: %t)",
				delivered.CorrelationId,
				message.User,
				message.FilePath,
				migrationId,
				message.EncryptedChecksums,
				message.ReVerify)

			// Send message to verified queue

			decryptedChecksum, err := db.GetDecryptedChecksum(message.FileID)
			if err != nil {
				log.Errorf("GetDecryptedChecksum failed (file-id: %s, corr-id: %s, user: %s, filepath: %s, reason: %v)",
					message.FileID, delivered.CorrelationId, message.User, message.FilePath, err)
			}
			c := verified{
				User:     message.User,
				FilePath: message.FilePath,
				DecryptedChecksums: []checksums{
					{"sha256", decryptedChecksum},
					//{"sha256", fmt.Sprintf("%x", sha256hash.Sum(nil))},
					//	{"md5", fmt.Sprintf("%x", md5hash.Sum(nil))},
				},
			}

			verifiedMessage, _ := json.Marshal(&c)

			err = mq.ValidateJSON(&delivered,
				"ingestion-accession-request",
				verifiedMessage,
				new(verified))

			if err != nil {
				log.Errorf("Validation (ingestion-accession-request) of outgoing message failed "+
					"(corr-id: %s, error: %v, message: %s)",
					delivered.CorrelationId,
					err,
					verifiedMessage)

				// Logging is in ValidateJSON so just restart on new message
				continue
			}

			if err := mq.SendMessage(delivered.CorrelationId,
				conf.Broker.Exchange,
				conf.Broker.RoutingKey,
				conf.Broker.Durable,
				verifiedMessage); err != nil {
				// TODO fix resend mechanism

				log.Errorf("Sending of message failed "+
					"(corr-id: %s, user: %s, filepath: %s, archivepath: %s, encryptedchecksums: %v, reverify: %t, reason: %v)",
					delivered.CorrelationId,
					message.User,
					message.FilePath,
					message.ArchivePath,
					message.EncryptedChecksums,
					message.ReVerify,
					err)

				continue
			}

			if err := delivered.Ack(false); err != nil {
				log.Errorf("Failed acking completed work"+
					"(corr-id: %s, user: %s, filepath: %s, archivepath: %s, encryptedchecksums: %v, reverify: %t, reason: %v)",
					delivered.CorrelationId,
					message.User,
					message.FilePath,
					message.ArchivePath,
					message.EncryptedChecksums,
					message.ReVerify,
					err)
			}
		}
	}()

	<-forever
}
