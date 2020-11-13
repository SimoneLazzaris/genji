package immudbengine
import (
	"context"
	"github.com/genjidb/genji/engine"
	immuclient "github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/api/schema"
	"google.golang.org/grpc/metadata"
	"errors"
)

type Engine struct {
	// private engine data goes here
	valid	bool
	closed	bool
	ctx	context.Context
	client	immuclient.ImmuClient
	md	metadata.MD
}

type immudbitem struct {
	key	[]byte
	val	[]byte
}

type transaction struct {
	valid	bool
	engine	*Engine
	ctx	context.Context
	writable bool
}


//-------

func NewEngine(host string, port int, username, password string) (eng *Engine, err error) {
	eng.valid = false
	opt := immuclient.DefaultOptions().WithAddress(host).WithPort(port)
	eng.client,err = immuclient.NewImmuClient(opt)
	if err != nil {
		return
	}
	eng.ctx = context.Background()
	lr , err := eng.client.Login(eng.ctx, []byte(username), []byte(password))
	if err != nil {
		return
	}
	
	eng.md = metadata.Pairs("authorization", lr.Token)
	eng.valid = true
	return
}

func (ng *Engine) Begin(ctx context.Context, opts engine.TxOptions) (engine.Transaction, error) {
	if !ng.valid {
		err := errors.New("Invalid engine")
		return nil, err
	}
	tx := transaction{
		ctx:	metadata.NewOutgoingContext(ctx, ng.md),
		writable: opts.Writable,
	}
	return &tx, nil
}

func (ng *Engine) Close() error {
	ng.client.Logout(ng.ctx)
	ng.valid=false
	return nil
}


func (tx *transaction) Rollback() error {
	return errors.New("rollback not supported on immudb")
}

func (tx *transaction) Commit() error {
	return nil
}

func (tx *transaction) GetStore(name []byte) (engine.Store, error) {
	if !tx.valid {
		return nil, errors.New("Invalid transaction")
	}
	select {
	case <-tx.ctx.Done():
		return nil, tx.ctx.Err()
	default:
	}
	
	dbReq := schema.Database{ Databasename: string(name) }
	
	repl, err := tx.engine.client.UseDatabase(tx.ctx, &dbReq)
	if err!=nil {
		return nil, engine.ErrStoreNotFound
	}
	
	md := metadata.Pairs("authorization", repl.Token)	
	ctx := metadata.NewOutgoingContext(context.Background(), md)
	
	return &store{ctx: ctx, tx: tx, name: string(name)}, nil
}

func (tx *transaction) CreateStore(name []byte) error {
	select {
	case <-tx.ctx.Done():
		return tx.ctx.Err()
	default:
	}

	if !tx.writable {
		return engine.ErrTransactionReadOnly
	}

	_, err := tx.GetStore(name)
	if err == nil {
		return engine.ErrStoreAlreadyExists
	}

	dbReq := schema.Database{ Databasename: string(name) }
	
	repl := tx.engine.client.CreateDatabase(tx.ctx, &dbReq)
	return nil
}

func (tx *transaction) DropStore(name []byte) error {
	return errors.New("rollback not supported on immudb")
}
