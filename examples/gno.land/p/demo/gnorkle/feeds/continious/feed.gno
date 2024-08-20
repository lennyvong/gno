package continious

import (
  "errors"
	"bufio"
	"bytes"

	"gno.land/p/demo/gnorkle/feed"
	"gno.land/p/demo/gnorkle/gnorkle"
	"gno.land/p/demo/gnorkle/ingesters/single"
	"gno.land/p/demo/gnorkle/message"
	"gno.land/p/demo/gnorkle/storage/simple"
	"gno.land/p/demo/ufmt"
)

type Feed struct {
	id string
	isActive bool
	valueDataType string
	ingester gnorkle.Ingester
	storage gnorkle.Storage
	tasks []feed.Task
}

func NewFeed(id string, valueDataType string, ingester gnorkle.Ingester, storage gnorkle.Storage, tasks ...feed.Task) *Feed {
	return &Feed{
		id: id,
		valueDataType: valueDataType,
		ingester: ingester,
		storage: storage,
		tasks: tasks,
		isActive: true,
	}
}

func NewSingleValueFeed(id string, valueDataType string, tasks ...feed.Task) *Feed {
	return NewFeed(id, valueDataType, &single.ValueIngester{}, simple.NewStorage(1), tasks...)
}

func (f Feed) ID() string {
	return f.id
}

func (f Feed) Type() feed.Type {
	return feed.TypeContinuous
}

func (f Feed) ValueDataType() string {
	return f.valueDataType
}

func (f *Feed) Ingest(funcType message.FuncType, msg, providerAddress string) error {
	if f == nil {
		return feed.ErrUndefined
	}

	switch funcType {
  case message.FuncTypeIngestCommit:
		if canAutoCommit, err := f.ingester.Ingest(msg, providerAddress); canAutoCommit && err == nil {
			if err := f.ingester.CommitValue(f.storage, providerAddress); err != nil {
				return err
			}
		} else if err != nil {
			return err
		}
	case message.FuncTypeIngest:
		if _, err := f.ingester.Ingest(msg, providerAddress); err != nil {
			return err
		}
	case message.FuncTypeCommit:
		if err := f.ingester.CommitValue(f.storage, providerAddress); err != nil {
			return err
		}
  default:
    return errors.New("invalid message function " + string(funcType))

	}
	return nil
}

func (f Feed) Value() (feed.Value, string, bool) {
	return f.storage.GetLatest(), f.valueDataType, f.isActive
}

func (f Feed) IsActive() bool {
	return f.isActive
}

func (f Feed) Tasks() []feed.Task {
	return f.tasks
}

// MarshalJSON marshals the components of the feed that are needed for
// an agent to execute tasks and send values for ingestion.
func (f Feed) MarshalJSON() ([]byte, error) {
	buf := new(bytes.Buffer)
	w := bufio.NewWriter(buf)

	w.Write([]byte(
		`{"id":"` + f.id +
			`","type":"` + ufmt.Sprintf("%d", int(f.Type())) +
			`","value_type":"` + f.valueDataType +
			`","tasks":[`),
	)

	first := true
	for _, task := range f.tasks {
		if !first {
			w.WriteString(",")
		}

		taskJSON, err := task.MarshalJSON()
		if err != nil {
			return nil, err
		}

		w.Write(taskJSON)
		first = false
	}

	w.Write([]byte("]}"))
	w.Flush()

	return buf.Bytes(), nil
}
