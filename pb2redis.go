package main

// BUILD instructions:
//   1. prerequisites
//        go get github.com/garyburd/redigo/redis
//        go get github.com/qedus/osmpbf
//   2. go build pb2redis.go

import (
    "bytes"
    "errors"
    "fmt"
    "github.com/garyburd/redigo/redis"
    "github.com/qedus/osmpbf"
    "io"
    "os"
    "runtime"
    "strings"
    "time"
)

const (
    c_max_num_chunk   = 5         // sleep if there are more than x chunks in the redis db
    c_sleeptime_millis= 30000     // sleeptime: half a minute
    c_chunk_size      = 128000000 // size of a chunk: a bit less than 128 megabyte 
                                  // (128 MB actually is 1.37439e+11 bytes)
)

func main() {

    // need 1 input argument: the OSM pb file
    if len(os.Args)<2 {
        fmt.Fprintf(os.Stderr,"No input filename given!\n")
        os.Exit(1)
    }
    pb_filename := os.Args[1]
    if len(pb_filename)<1 {
        fmt.Fprintf(os.Stderr,"No input filename given!\n")
        os.Exit(1)
    }

    // if no REDIS_URL defined in the env, then use localhost
    redisUrl:=os.Getenv("REDIS_URL")
    if len(redisUrl)==0 {
        redisUrl="localhost:6379"
    }

     // connect to redis
    conn, err := redis.Dial("tcp", redisUrl)
    if err != nil {
        fmt.Fprintf(os.Stderr,"Error: %v\n", err)
        os.Exit(1)
    }
    defer conn.Close()

    // refuse to start if already data in buffer!
    var val int
    val, err = redis.Int(conn.Do("LLEN", "TR_QUEUE"))
    if (err!=nil) {
        fmt.Fprintf(os.Stderr,"Error: %v\n", err)
        os.Exit(1)
    }
    if (val>0) {
        fmt.Fprintf(os.Stderr,"Error: buffer already contains values!\n")
        os.Exit(1)
    }


    // raise flag, to tell the chunk reader on the other side to stay connected
    // --- put in TR_BUFFER -----------------------------------------------
    _, err = conn.Do("SADD", "TR_FLAG", "RAISE" ) 
    if err != nil {
        fmt.Fprintf(os.Stderr,"Error: %v\n", err)
        os.Exit(1)
    }

    // start reading the file
    f, err := os.Open(pb_filename)
    if err != nil {
        fmt.Fprintf(os.Stderr,"Error: %v\n", err)
        os.Exit(1)
    }
    defer f.Close()

    d := osmpbf.NewDecoder(f)
    err = d.Start(runtime.GOMAXPROCS(-1))
    if err != nil {
        fmt.Fprintf(os.Stderr,"Error: %v\n", err)
        os.Exit(1)
    }
    var buf bytes.Buffer
    var nc, wc, rc, seq uint64
    seq=100000
    for {
        if v, err := d.Decode(); err == io.EOF {
            break
        } else if err != nil {
            fmt.Fprintf(os.Stderr,"Error: %v\n", err)
            os.Exit(1)
        } else {
            switch v := v.(type) {
            case *osmpbf.Node:
                tgs := ""
                // build the tags string
                for k, v := range v.Tags {
                    tgs = tgs + " " + k + ":" + v
                }
                tgs=strings.TrimSpace( strings.Replace(tgs, "\t"," ",-1) )
                if len(tgs)>0 {
                    buf.WriteString(fmt.Sprintf("%f\t%f\t%s\n", v.Lat, v.Lon, tgs) )
                }
                if (buf.Len()>c_chunk_size) {
                    insert_in_redis(conn,buf, fmt.Sprintf("%d.csv",seq))
                    buf.Reset()
                    seq+=1
                }
                nc++
            case *osmpbf.Way:
                // Way : ignored
                wc++
            case *osmpbf.Relation:
                // Relation : ignored
                rc++
            default:
                fmt.Fprintf(os.Stderr, "unknown type %T\n", v)
                os.Exit(1)
            }
        }
    }
    // handle the last buffer
    if (buf.Len()>0) {
        insert_in_redis(conn,buf, fmt.Sprintf("%d.csv",seq))
    }
    fmt.Printf("# Nodes: %d, Ways: %d, Relations: %d\n", nc, wc, rc)

    // tell client to stay connected no longer
    _, err = conn.Do("DEL", "TR_FLAG" )
    if err != nil {
        fmt.Fprintf(os.Stderr,"Error: %v\n", err)
    }
}


func insert_in_redis(conn redis.Conn, buf bytes.Buffer, key string) (err error) {
    var val int
    // --- sleep if too many entries in TR_QUEUE --------------------------
    for {
        val, err = redis.Int(conn.Do("LLEN", "TR_QUEUE"))
        if (err!=nil) { return }
        if (val>c_max_num_chunk) {
            fmt.Printf(".")
            time.Sleep(time.Duration(c_sleeptime_millis) * time.Millisecond)
        } else {
            fmt.Println()
            break
        }
    }

    // --- put in TR_BUFFER -----------------------------------------------
    val, err = redis.Int(conn.Do("HSET", "TR_BUFFER", key, buf.String()))
    if (err!=nil) { return }
    if (val==0) {
        err=errors.New("Error writing chunk to redis")
        return
    }

    // --- left push the key onto the TR_QUEUE ------------------------------
    val, err = redis.Int(conn.Do("LPUSH", "TR_QUEUE", key))
    if (err!=nil) { fmt.Printf("Error: %v\n",err); return }
    if (val==0) {
        err=errors.New("Error pushing key on queue in redis") 
        return
    }
    fmt.Printf("Chunk %s handled.\n", key)
    return
}

