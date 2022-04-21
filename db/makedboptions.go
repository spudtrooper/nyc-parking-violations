package db

//go:generate genopts --prefix=MakeDB --outfile=makedboptions.go "port:int" "dbName:string"

type MakeDBOption func(*makeDBOptionImpl)

type MakeDBOptions interface {
	Port() int
	DbName() string
}

func MakeDBPort(port int) MakeDBOption {
	return func(opts *makeDBOptionImpl) {
		opts.port = port
	}
}
func MakeDBPortFlag(port *int) MakeDBOption {
	return func(opts *makeDBOptionImpl) {
		opts.port = *port
	}
}

func MakeDBDbName(dbName string) MakeDBOption {
	return func(opts *makeDBOptionImpl) {
		opts.dbName = dbName
	}
}
func MakeDBDbNameFlag(dbName *string) MakeDBOption {
	return func(opts *makeDBOptionImpl) {
		opts.dbName = *dbName
	}
}

type makeDBOptionImpl struct {
	port   int
	dbName string
}

func (m *makeDBOptionImpl) Port() int      { return m.port }
func (m *makeDBOptionImpl) DbName() string { return m.dbName }

func makeMakeDBOptionImpl(opts ...MakeDBOption) *makeDBOptionImpl {
	res := &makeDBOptionImpl{}
	for _, opt := range opts {
		opt(res)
	}
	return res
}

func MakeMakeDBOptions(opts ...MakeDBOption) MakeDBOptions {
	return makeMakeDBOptionImpl(opts...)
}
