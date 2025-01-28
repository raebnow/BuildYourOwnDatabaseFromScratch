package main

import (
    "os"
    "fmt"
    "log"
    "math/rand"
    "time"
)

func SaveData1(path string, data []byte) error {
    fp, err := os.OpenFile(path, os.O_WRONLY | os.O_CREATE | os.O_TRUNC, 0664)
    if err != nil {
        return err
    }

    defer fp.Close()

    _, err = fp.Write(data)
    return err
}

func SaveData2(path string, data []byte) error {
    tmp := fmt.Sprintf("%s.tmp.%d", path, randomInt())
    fp, err := os.OpenFile(tmp, os.O_WRONLY | os.O_CREATE | os.O_EXCL, 0664)
    if err != nil {
        return err
    }

    _, err = fp.Write(data)
    if err != nil {
        os.Remove(tmp)
        return err
    }

    fp.Close()

    return os.Rename(tmp, path)
}

func randomInt() int {
    rand.Seed(time.Now().UnixNano())
    return rand.Int()
}

func SaveData3(path string, data []byte) error {
    tmp := fmt.Sprintf("%s.tmp.%d", path, randomInt())
    fp, err := os.OpenFile(tmp, os.O_WRONLY | os.O_CREATE | os.O_EXCL, 0664)
    if err != nil {
        return err
    }

    _, err = fp.Write(data)
    if err != nil {
        os.Remove(tmp)
        return err
    }

    err = fp.Sync()
    if err != nil {
        os.Remove(tmp)
        return err
    }

    fp.Close()
    
    return os.Rename(tmp, path)
}

func LogCreate(path string) (*os.File, error) {
    return os.OpenFile(path, os.O_RDWR | os.O_CREATE, 0664)
}

func LogAppend(fp *os.File, line string) error {
    buf := []byte(line)
    buf = append(buf, '\n')

    _, err := fp.Write(buf)

    if err != nil {
        return err
    }

    return fp.Sync()
}

func main() {
    path := "example.txt"
    data1 := []byte("Hello, World! This is a test file.")
    data2 := []byte("This is additional data saved using SaveData2.\n")
    data3 := []byte("This is data saved using SaveData3 with disk synchronization.\n")

    err := SaveData1(path, data1)
    if err != nil {
        log.Fatalf("Failed to save data with SaveData1: %v", err)
    }

    fmt.Printf("Data successfully saved to %s using SaveData1\n", path)

    err = SaveData2(path, data2)
    if err != nil {
        log.Fatalf("Failed to save data with SaveData2: %v", err)
    }
    fmt.Printf("Data successfully saved to %s using SaveData2\n", path)

    err = SaveData3(path, data3)
    if err != nil {
        log.Fatalf("Failed to save data with SaveData3: %v", err)
    }
    fmt.Printf("Data succesfully saved to %s using SaveData3\n", path)
    
    fmt.Println("Final file contents:")

    contents, err := os.ReadFile(path)
    if err != nil {
        log.Fatalf("Failed to read file: %v", err)
    }
    fmt.Println(string(contents))

    logFilePath := "log.txt"

    logFile, err := LogCreate(logFilePath)
    if err != nil {
        log.Fatalf("Failed to create or open log file: %v", err)
    }
    defer logFile.Close()

    err = LogAppend(logFile, "Program started.")
    if err != nil {
        log.Fatalf("Failed to write to log file: %v", err)
    }
}
