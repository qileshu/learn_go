package main

import "fmt"
import "os"

func main(){
    fileName := "1.txt"
    wFile,err := os.Create(fileName)
    if err != nil{
        fmt.Println(err.Error())
        return
    }
    defer wFile.Close()
    str := "hello world"
    wFile.WriteString(str)
}


