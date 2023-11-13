package adb

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/arangodb/go-driver"
	"github.com/arangodb/go-driver/http"
	"github.com/pkg/errors"
	"time"
)

// InitArangoDB инициализирует подключение к БД
// если БД не существует то она будет создана
func InitArangoDB(ctx context.Context, cnf Config) (driver.Database, error) {
	var db driver.Database
	conn, err := http.NewConnection(http.ConnectionConfig{
		Endpoints: cnf.Endpoints,
	})
	if err != nil {
		return nil, errors.Wrap(err, "ошибка инициализации HTTP соединения")
	}
	client, err := driver.NewClient(driver.ClientConfig{
		Connection:     conn,
		Authentication: driver.BasicAuthentication(cnf.User, cnf.Password),
	})
	if err != nil {
		return nil, errors.Wrap(err, "ошибка инициализации клиента ArangoDB")
	}
	ex, err := client.DatabaseExists(ctx, cnf.Database)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("ошибка при проверки существования базы данных `%s`", cnf.Database))
	}
	if ex == false {
		users := make([]driver.CreateDatabaseUserOptions, 1)
		active := true
		users[0] = driver.CreateDatabaseUserOptions{
			UserName: cnf.User,
			Password: cnf.Password,
			Active:   &active,
		}
		db, err = client.CreateDatabase(ctx, cnf.Database, &driver.CreateDatabaseOptions{
			Users:   users,
			Options: driver.CreateDatabaseDefaultOptions{},
		})
		if err != nil {
			return nil, errors.Wrap(err, fmt.Sprintf("не смог создать базу данных `%s`", cnf.Database))
		}
	} else {
		db, err = client.Database(ctx, cnf.Database)
		if err != nil {
			return nil, errors.Wrap(err, fmt.Sprintf("не смог подключиться к базе данных `%s`", cnf.Database))
		}
	}

	return db, nil
}

// InitArangoDbCollection инициализирует коллекцию
// если коллекции не существует то она будет создана
func InitArangoDbCollection(
	ctx context.Context,
	adb driver.Database,
	colName string,
	opt *driver.CreateCollectionOptions,
) (created bool, col driver.Collection, err error) {
	ex, err := adb.CollectionExists(ctx, colName)
	if err != nil {
		err = errors.Wrap(err, fmt.Sprintf("ошибка при проверки существования коллекции %s в БД %s", colName, adb.Name()))
		return
	}

	if ex {
		col, err = adb.Collection(ctx, colName)
		if err != nil {
			err = errors.Wrap(err, fmt.Sprintf("ошибка при подключении к коллекции %s в БД %s", adb.Name(), colName))
		}
		return
	}

	col, err = adb.CreateCollection(ctx, colName, opt)
	if err != nil {
		err = errors.Wrap(err, fmt.Sprintf("ошибка при создании коллекции %s в БД %s", colName, adb.Name()))
		return
	}
	created = true
	return
}

// InitArangoDbPersistentIndex инициализирует PersistentIndex в коллекции
// если PersistentIndex не существует то он будет создан
func InitArangoDbPersistentIndex(ctx context.Context, col driver.Collection, opt struct {
	Cols []string
	Opt  *driver.EnsurePersistentIndexOptions
}) (created bool, err error) {
	ex, err := col.IndexExists(ctx, opt.Opt.Name)
	if err != nil {
		err = errors.Wrap(err, fmt.Sprintf("ошибка при проверки существования индекса %s в коллекции %s.%s", opt.Opt.Name, col.Database().Name(), col.Name()))
		return
	}
	if ex {
		return
	}

	_, created, err = col.EnsurePersistentIndex(ctx, opt.Cols, opt.Opt)
	if err != nil {
		err = errors.Wrap(err, fmt.Sprintf("ошибка при создании индекса %s в коллекции %s.%s", opt.Opt.Name, col.Database().Name(), col.Name()))
	}
	return
}

// InitArangoDbEnsureTTLIndex инициализация EnsureTTLIndex в коллекции
// если EnsureTTLIndex не существует то он будет создан
func InitArangoDbEnsureTTLIndex(ctx context.Context, col driver.Collection, opt struct {
	Field string        // Имя поля с типом time.Time
	TTL   time.Duration // Время жизни в секундах от времени указанном в поле Field
	Opt   *driver.EnsureTTLIndexOptions
}) (created bool, err error) {
	ex, err := col.IndexExists(ctx, opt.Opt.Name)
	if err != nil {
		err = errors.Wrap(err, fmt.Sprintf("ошибка при проверки существования индекса %s в коллекции %s.%s", opt.Opt.Name, col.Database().Name(), col.Name()))
		return
	}
	if ex {
		return
	}

	_, created, err = col.EnsureTTLIndex(ctx, opt.Field, int(opt.TTL.Seconds()), opt.Opt)
	if err != nil {
		err = errors.Wrap(err, fmt.Sprintf("ошибка при создании индекса %s в коллекции %s.%s", opt.Opt.Name, col.Database().Name(), col.Name()))
	}
	return
}

func StructToMap(structPtr any) (map[string]any, error) {
	raw, err := json.Marshal(structPtr)
	if err != nil {
		return nil, err
	}

	var data map[string]any
	err = json.Unmarshal(raw, &data)
	return data, err
}
