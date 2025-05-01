/*
 * Meilindex - mail indexing and search tool.
 * Copyright (C) 2020 Tero Vierimaa
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 *
 */

package indexer

import (
	"crypto/md5"
	"errors"
	"fmt"
	"github.com/kmorey/meilindex/config"
	"github.com/meilisearch/meilisearch-go"
	"github.com/sirupsen/logrus"
	"runtime"
	"strings"
	"sync"
	"time"
)

// NewMeilisearch creates new connection.
func NewMeiliSearch() (*Meilisearch, error) {
	m := &Meilisearch{
		Url:           config.Conf.Meilisearch.Url,
		Index:         config.Conf.Meilisearch.Index,
		ApiKey:        config.Conf.Meilisearch.ApiKey,
		maxNumPushers: runtime.NumCPU(),
	}
	m.pushDone = make(chan bool, m.maxNumPushers)
	err := m.Connect()
	return m, err
}

// Meilisearch is a connector to Meilisearch.
type Meilisearch struct {
	Url    string
	Index  string
	ApiKey string
	client meilisearch.ServiceManager

	lock          sync.Mutex
	numPushers    int
	maxNumPushers int
	pushDone      chan bool
}

// Connect creates a connection to meilisearch instance and initializes index if neccessary.
func (m *Meilisearch) Connect() error {
	m.client = meilisearch.New(
		m.Url,
		meilisearch.WithAPIKey(m.ApiKey),
	)

	version, err := m.ServerVersion()
	if err != nil {
		return fmt.Errorf("get server version: %v", err)
	}

	logrus.Infof("Meilisearch version: %s", version)

	index := m.client.Index(m.Index)
	if index == nil {
		if e, ok := err.(*meilisearch.Error); ok {
			if e.StatusCode == 404 {
				err = nil
			}
		}
	}

	if err != nil {
		return fmt.Errorf("get indexes: %v", err)
	}

	return nil
}

func (m *Meilisearch) ServerVersion() (string, error) {
	v, err := m.client.Version()
	if err != nil {
		return "", err
	}

	if v != nil {
		return v.PkgVersion, err
	}

	return "", fmt.Errorf("empty version, %v", err)
}

// IndexMailBackground runs multiple goroutines (num of cpus) to push mails to meilisearch.
// If all goroutines are busy, this call blocks as long as some goroutine is available.
func (m *Meilisearch) IndexMailBackground(mail []*Mail) error {
	started := time.Now()
	for {
		ready := time.Now()
		if m.pusherAvailable() {
			logrus.Debugf("Meilisearch pusher available after %d ms", ready.Sub(started).Milliseconds())
			m.startNewPusher(mail)
			break
		} else {
			time.Sleep(time.Millisecond * 5)
		}
		if ready.Sub(started).Seconds() > 3600 {
			return fmt.Errorf("timeout waiting for available pusher")
		}
	}
	return nil
}

func (m *Meilisearch) pusherAvailable() bool {
	m.lock.Lock()
	defer m.lock.Unlock()
	return m.numPushers < m.maxNumPushers
}

func (m *Meilisearch) startNewPusher(mails []*Mail) {
	m.lock.Lock()
	m.numPushers += 1
	m.lock.Unlock()

	go func() {
		err := m.indexMail(mails, true)
		if err != nil {
			logrus.Errorf("index mails: %v", err)
		}
		m.lock.Lock()
		m.numPushers -= 1
		m.lock.Unlock()
	}()
}

func (m *Meilisearch) IndexComplete() bool {
	m.lock.Lock()
	defer m.lock.Unlock()
	return m.numPushers == 0
}

func (m *Meilisearch) WaitIndexComplete() {
	for {
		if m.IndexComplete() {
			return
		}
		time.Sleep(time.Millisecond * 10)
	}
}

func (m *Meilisearch) IndexMail(mails []*Mail) error {
	return m.indexMail(mails, false)
}

// IndexMail indexes new mail or updates existing mails.
func (m *Meilisearch) indexMail(mail []*Mail, background bool) error {

	logrus.Infof("Index %d mails", len(mail))

	documents := make([]map[string]interface{}, len(mail))

	for i, v := range mail {
		v.Sanitize()
		doc := map[string]interface{}{}
		doc["id"] = v.Id
		doc["date"] = v.Timestamp.Unix()
		doc["from"] = v.From
		doc["to"] = v.To
		doc["cc"] = v.Cc
		doc["subject"] = v.Subject
		doc["message"] = v.Body
		doc["folder"] = v.Folder
		doc["attachments"] = strings.Join(v.AttachmentNames, ",")
		documents[i] = doc

		// email ids can be too complex for meilisearch. Use md5 as a unique id for mail.
		hash := md5.Sum([]byte(v.Uid))
		doc["uid"] = fmt.Sprintf("%x", hash)
	}

	res, err := m.client.Index(m.Index).AddDocuments(documents)

	if err != nil {
		var meiliError *meilisearch.Error
		if errors.As(err, &meiliError) {
			msg := meiliError.MeilisearchApiError.Message
			code := meiliError.StatusCode
			expectedCode := meiliError.StatusCodeExpected
			err = fmt.Errorf("push %d emails: expected status: %d, got status: %d: %s",
				len(documents), expectedCode, code, msg)
			return err
		}
	} else {
		logrus.Debug("Meilisearch update id: ", res.IndexUID)
		logrus.Infof("Created / updated %d mails", len(mail))
	}
	return nil
}

// RankingRules returns a list of ranking rules. First rule is the most important, last is least important.
func (m *Meilisearch) RankingRules() (*[]string, error) {

	return m.client.Index(m.Index).GetRankingRules()
}

func (m *Meilisearch) SetRankingRules(rules *[]string) error {
	_, err := m.client.Index(m.Index).UpdateRankingRules(rules)
	return err
}

// StopWords returns all stop words currently being used.
func (m *Meilisearch) StopWords() (*[]string, error) {
	words, err := m.client.Index(m.Index).GetStopWords()
	if err != nil {
		return nil, err
	}
	return words, nil
}

func (m *Meilisearch) SetStopWords(words *[]string) error {
	_, err := m.client.Index(m.Index).UpdateStopWords(words)
	return err
}

func (m *Meilisearch) Synonyms() (*map[string][]string, error) {
	synonyms, err := m.client.Index(m.Index).GetSynonyms()
	if err != nil {
		return nil, err
	}
	return synonyms, nil
}

func (m *Meilisearch) SetSynonyms(synonyms *map[string][]string) error {
	_, err := m.client.Index(m.Index).UpdateSynonyms(synonyms)
	return err
}

func (m *Meilisearch) Stats() ServerStats {
	stats, err := m.client.GetStats()
	if err != nil {
		logrus.Errorf("get stats: %v", err)
	}

	indexStats := stats.Indexes[m.Index]
	serverStats := ServerStats{
		NumDocuments: indexStats.NumberOfDocuments,
		Indexing:     indexStats.IsIndexing,
	}

	version, err := m.ServerVersion()
	if err == nil {
		serverStats.ServerVersion = version
	} else {
		serverStats.ServerVersion = "-"
	}

	return serverStats
}

type ServerStats struct {
	NumDocuments  int64
	Indexing      bool
	ServerVersion string
}
