package p1

import (
	"bufio"
	"net"
	"strconv"
	"strings"
)

const maxMsg = 500


type mensagem struct {
	chave    string
	valor  []byte
}

type cliente struct {
	connection   net.Conn
	mensagemQ chan mensagem
	sairS   chan bool
	temGet        chan mensagem
	temPut        chan mensagem
	clientError  chan *cliente
	reader       *bufio.Reader
}

type keyValueServer struct {
	servidor              net.Listener
	clientes   chan net.Conn
	temGet           chan mensagem
	temPut           chan mensagem
	paraBD    chan mensagem
	msgT     chan mensagem
	qtdClentes chan int
	clientError     chan *cliente
	sairS      chan bool
	quitRunner      chan bool
}

// New cria e retorna (mas não inicia) um KeyValueServer.
func New() KeyValueServer {
	return &keyValueServer{
		clientes:   make(chan net.Conn),
		qtdClentes: make(chan int),
		paraBD:    make(chan mensagem),
		temGet:           make(chan mensagem),
		temPut:           make(chan mensagem),
		msgT:     make(chan mensagem, maxMsg),
		clientError:     make(chan *cliente),
		sairS:      make(chan bool),
		quitRunner:      make(chan bool),
	}
}

func (kvs *keyValueServer) Start(port int) error {
	servidor, err := net.Listen("tcp", ":"+strconv.Itoa(port))
	if err != nil {
		return err
	}
	init_db()
	kvs.servidor = servidor
	clients := make([]*cliente, 0)
	go BD(kvs.temGet, kvs.temPut, kvs.paraBD)
	go kvs.Listen(clients)
	go kvs.ex()
	return nil
}

func (kvs *keyValueServer) Count() int {
	kvs.clientes <- nil
	return <-kvs.qtdClentes
}

func (kvs *keyValueServer) Close() {
	kvs.sairS <- true
}

func (kvs *keyValueServer) ex() error {
	for {
		select {
		case <-kvs.quitRunner:
			return nil
		default:
			conn, err := kvs.servidor.Accept()
			if err == nil || conn != nil {
				kvs.clientes <- conn
			}
		}
	}
}

func (kvs *keyValueServer) Listen(clients []*cliente) {
	for {
		select {
		case conn := <-kvs.clientes:
			if conn == nil {
				kvs.qtdClentes <- len(clients)
			} else {
				cliente := &cliente{
					connection:   conn,
					mensagemQ: make(chan mensagem, maxMsg),
					reader:       bufio.NewReader(conn),
					sairS:   make(chan bool),
					temGet:        kvs.temGet,
					temPut:        kvs.temPut,
					clientError:  kvs.clientError,
				}
				go cliente.ClientListen()
				go wirter(cliente)
				clients = append(clients, cliente)
			}
		case <-kvs.sairS:
			kvs.servidor.Close()
			kvs.quitRunner <- true
			for _, c := range clients {
				c.Close()
			}
		case cError := <-kvs.clientError:
			for i, c := range clients {
				if c == cError {
					clients = append(clients[:i], clients[i+1:]...)
					break
				}
			}
		case query := <-kvs.paraBD:
			enviaT(clients, query)
		}
	}
}
//consul ao bd
func BD(clientGet chan mensagem, clientPut chan mensagem, msgT chan mensagem) {
	for {
		select {
		case query := <-clientGet:
			query.valor = get(query.chave)
			msgT <- query
		case data := <-clientPut:
			put(data.chave, data.valor)
		}
	}
}
//envia a todos a query do bd 
func enviaT(clients []*cliente, query mensagem) {
	for _, cliente := range clients {
		if len(cliente.mensagemQ) != 500 {
			cliente.mensagemQ <- query
		}
	}
}

//msg recebidas do cliente
func (c *cliente) ClientListen() {
	for {
		select {
		case <-c.sairS:
			return
		default:
			msg, err := c.reader.ReadString('\n')

			if err != nil {
				c.clientError <- c
				return
			}
			tokens := strings.Split(msg, ",")
			if string(tokens[0]) == "put" {
				c.temPut <- mensagem{
					chave:   strings.TrimSpace(tokens[1]),
					valor: []byte(tokens[2]),
				}
			} else {
				chave := strings.TrimSpace(tokens[1])
				c.temGet <- mensagem{
					chave: chave,
				}
			}
		}
	}
}

func wirter(c *cliente) {
	for {
		response := <-c.mensagemQ
		c.connection.Write(append(append([]byte(response.chave), ","...), response.valor...))
	}
}

func (c *cliente) Close() {
	c.connection.Close()
	c.sairS <- true
}
