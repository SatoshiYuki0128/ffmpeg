package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"math/big"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	_ "github.com/go-sql-driver/mysql"
)

var (
	//資料夾路徑
	queueDir = "/Users/kiva_yang/Desktop/Sites/Go/src/video2m3u8/yellow-video/queue"
	imageDir = "/Users/kiva_yang/Desktop/Sites/Go/src/video2m3u8/yellow-video/videobackend-api/image"
	videoDir = "/Users/kiva_yang/Desktop/Sites/Go/src/video2m3u8/yellow-video/videobackend-api/videos"
	aesDir   = "/Users/kiva_yang/Desktop/Sites/Go/src/video2m3u8/yellow-video/transdone"
	keyDir   = "/Users/kiva_yang/Desktop/Sites/Go/src/video2m3u8/yellow-video/key"
	env      = "prod"

	//DB
	host     = "localhost"
	user     = "root"
	database = "database"
	password = "password"

	//騰訊雲
	cosName   = "cosName"
	cosRegion = "cosRegion"
	secretId  = "secretId"
	secretKey = "secretKey"

	//阿里雲
	endPoint        = "endPoint"
	accessKeyId     = "accessKeyId"
	accessKeySecret = "accessKeySecret"
	myBucket        = "myBucket"
)

func main() {

	c := cron.New()
	lock := false
	fmt.Println("start cron job :")
	c.AddFunc("@every 30s", func() {
		scanQueueFile(&lock)
	})
	c.Start()
	for {
		time.Sleep(time.Second)
	}

	//scanQueueFile(&lock)
}

// 掃描queueFile
func scanQueueFile(lock *bool) {

	//判斷程式是否執行中
	if *lock {
		//fmt.Println("程式執行中！")
		return
	}

	//上鎖
	*lock = true

	//讀取資料夾內檔案
	queuefileNames := getFileNames(queueDir + "/" + env)

	//掃描記錄
	if len(queuefileNames) > 0 {
		log := fmt.Sprintf("在 %s 時找到 %d 個檔案。", time.Now(), len(queuefileNames))
		fmt.Println(log)
	}

	imagePath, videoPath := []string{}, []string{}

	//取影片及圖片路徑
	for i := range queuefileNames {

		imagePath = append(imagePath, getImagePath(queueDir+"/"+env+"/"+queuefileNames[i]))
		videoPath = append(videoPath, getVideoPath(queueDir+"/"+env+"/"+queuefileNames[i]))

		//刪除queueFile
		/*err := exec.Command("rm", queueDir+"/"+env+"/"+queuefileNames[i]).Run()
		if err != nil {
			fmt.Println(err)
		}*/
	}

	//轉檔
	for i := range queuefileNames {

		fmt.Println(queuefileNames[i] + " : 轉檔中...")
		start := time.Now()

		//圖片轉檔
		if imagePath[i] != "" && !Exists(aesDir+"/"+env+"/"+queuefileNames[i][:len(queuefileNames[i])-15]+"/image") {
			imageTransfer(imagePath[i])
		}

		//影片轉檔
		if videoPath[i] != "" && !Exists(aesDir+"/"+env+"/"+queuefileNames[i][:len(queuefileNames[i])-15]+"/video") {
			videoTransfer(videoPath[i])
		}

		//更新proccess status
		if len(getDir(aesDir+"/"+env+"/"+queuefileNames[i][:len(queuefileNames[i])-15])) > 1 {
			updateProccessStatus(queuefileNames[i])
		}

		//記錄執行時間
		log := fmt.Sprintf("%s : 轉檔完成, 耗時 %d 秒。", queuefileNames[i], time.Since(start)/1000/1000/1000)
		fmt.Println(log)
	}

	//解鎖
	*lock = false
}

// 判斷檔案是否存在
func Exists(path string) bool {
	_, err := os.Stat(path)
	if err != nil {
		return os.IsExist(err)
	}
	return true
}

// 更新proccess status
func updateProccessStatus(fileNames string) {

	//建立db連線
	db := DbConnect()
	defer db.Close()

	//抓出id
	id := ""
	flag := false
	for i := range fileNames {
		if flag {
			if fileNames[i] != '-' {
				id += string(fileNames[i])
			} else {
				break
			}
		} else {
			if fileNames[i] == '-' {
				flag = true
			}
		}
	}

	//fmt.Println(id)

	//寫入db
	sql := fmt.Sprintf("update Videos set processStatus = 1 where id = %s;", id)
	rows, err := db.Query(sql)
	if err != nil {
		fmt.Println(sql)
	}
	defer rows.Close()

}

// 取得路徑內目錄
func getDir(imageDir string) []string {
	originImage := []string{}
	files, _ := ioutil.ReadDir(imageDir)
	for i := range files {
		originImage = append(originImage, files[i].Name())
	}
	return originImage
}

// 影片轉檔
func videoTransfer(videoPath string) {
	//建立資料夾
	mkdir(keyDir + "/" + env + "/" + videoPath[:len(videoPath)-19])
	mkdir(keyDir + "/" + env + "/" + videoPath[:len(videoPath)-19] + "/video")

	//寫入key檔
	keyfileName := keyDir + "/" + env + "/" + videoPath[:len(videoPath)-19] + "/video/" + videoPath[:len(videoPath)-19] + ".key"
	videoKey := getRandomVideoKey()
	writeVideoKey(keyfileName, videoKey)

	//建立資料夾
	mkdir(aesDir + "/" + env + "/" + videoPath[:len(videoPath)-19])
	mkdir(aesDir + "/" + env + "/" + videoPath[:len(videoPath)-19] + "/video")

	//創建初始keyFile檔
	keyInfoFileName := keyDir + "/" + env + "/" + videoPath[:len(videoPath)-19] + "/video/" + videoPath[:len(videoPath)-19] + ".keyinfo"
	writeVideoKeyInfo(keyInfoFileName, keyfileName, "localhost", videoPath[:len(videoPath)-19])

	//帶入command參數
	cmdPar := []string{}
	cmdPar = append(cmdPar, "-i")
	cmdPar = append(cmdPar, videoDir+"/"+env+"/"+videoPath)
	cmdPar = append(cmdPar, "-hls_time")
	cmdPar = append(cmdPar, "10")
	cmdPar = append(cmdPar, "-hls_list_size")
	cmdPar = append(cmdPar, "0")
	cmdPar = append(cmdPar, "-hls_segment_filename")
	cmdPar = append(cmdPar, aesDir+"/"+env+"/"+videoPath[:len(videoPath)-19]+"/video/"+videoPath[:len(videoPath)-19]+"_%5d.ts")
	cmdPar = append(cmdPar, "-hls_key_info_file")
	cmdPar = append(cmdPar, keyInfoFileName)
	cmdPar = append(cmdPar, aesDir+"/"+env+"/"+videoPath[:len(videoPath)-19]+"/video/"+videoPath[:len(videoPath)-19]+".m3u8")

	//執行
	err := exec.Command("ffmpeg", cmdPar...).Run()
	if err != nil {
		fmt.Println(err)
	}

	//所有domain都複製一份
	apiDomain := getApiDomain()
	for i := range apiDomain {
		//開檔
		f, _ := os.OpenFile(aesDir+"/"+env+"/"+videoPath[:len(videoPath)-19]+"/video/"+videoPath[:len(videoPath)-19]+".m3u8", os.O_RDONLY, 0600)
		m3u8Byte, _ := ioutil.ReadAll(f)

		//替換localhost
		newM3u8 := strings.Replace(string(m3u8Byte), "localhost", apiDomain[i], 1)

		//寫入檔案
		newFileName := aesDir + "/" + env + "/" + videoPath[:len(videoPath)-19] + "/video/" + videoPath[:len(videoPath)-19] + "." + apiDomain[i] + ".m3u8"
		f, err := os.Create(newFileName)
		if err != nil {
			fmt.Println(err)
		}
		defer f.Close()
		f.Write([]byte(newM3u8))
	}

	//上傳騰訊雲

	uploadVideo(videoPath[:len(videoPath)-19])

}

// 上傳影片至雲端
func uploadVideo(fileNames string) {

	u, _ := url.Parse("https://" + cosName + ".cos." + cosRegion + ".myqcloud.com")
	b := &cos.BaseURL{BucketURL: u}
	c := cos.NewClient(b, &http.Client{
		Transport: &cos.AuthorizationTransport{
			SecretID:  secretId,
			SecretKey: secretKey,
		},
	})

	//騰訊雲
	//上傳m3u8及ts
	uploadFiles := getFileNames(aesDir + "/" + env + "/" + fileNames + "/video")
	for i := range uploadFiles {
		key := fileNames + "/video/" + uploadFiles[i]
		localFile := aesDir + "/" + env + "/" + fileNames + "/video/" + uploadFiles[i]
		_, err := c.Object.PutFromFile(context.Background(), key, localFile, nil)
		if err != nil {
			panic(err)
		}
	}
	//上傳key
	key := "key" + "/" + fileNames + "/video/" + fileNames + ".key"
	localFile := keyDir + "/" + env + "/" + fileNames + "/video/" + fileNames + ".key"
	_, err := c.Object.PutFromFile(context.Background(), key, localFile, nil)
	if err != nil {
		panic(err)
	}

	client, err := oss.New(endPoint, accessKeyId, accessKeySecret)
	if err != nil {
		fmt.Println(err)
	}
	bucket, err := client.Bucket(myBucket)
	if err != nil {
		fmt.Println(err)
	}

	//阿里雲
	//m3u8及ts
	for i := range uploadFiles {
		key := fileNames + "/video/" + uploadFiles[i]
		localFile := aesDir + "/" + env + "/" + fileNames + "/video/" + uploadFiles[i]
		err = bucket.PutObjectFromFile(key, localFile)
		if err != nil {
			fmt.Println(err)
		}
	}
	//key
	key = "key" + "/" + fileNames + "/video/" + fileNames + ".key"
	localFile = keyDir + "/" + env + "/" + fileNames + "/video/" + fileNames + ".key"
	err = bucket.PutObjectFromFile(key, localFile)
	if err != nil {
		fmt.Println(err)
	}

}

// 上傳圖片至雲端
func uploadImage(fileNames string) {

	//騰訊雲
	u, _ := url.Parse("https://" + cosName + ".cos." + cosRegion + ".myqcloud.com")
	b := &cos.BaseURL{BucketURL: u}
	c := cos.NewClient(b, &http.Client{
		Transport: &cos.AuthorizationTransport{
			SecretID:  secretId,
			SecretKey: secretKey,
		},
	})
	//上傳aes
	key := fileNames + "/image/" + fileNames + ".aes"
	localFile := aesDir + "/" + env + "/" + key
	_, err := c.Object.PutFromFile(context.Background(), key, localFile, nil)
	if err != nil {
		panic(err)
	}
	//上傳key
	key = "key" + "/" + fileNames + "/image/" + fileNames + ".key"
	localFile = keyDir + "/" + env + "/" + fileNames + "/image/" + fileNames + ".key"
	_, err = c.Object.PutFromFile(context.Background(), key, localFile, nil)
	if err != nil {
		panic(err)
	}

	//阿里雲
	client, err := oss.New(endPoint, accessKeyId, accessKeySecret)
	if err != nil {
		fmt.Println(err)
	}
	bucket, err := client.Bucket(myBucket)
	if err != nil {
		fmt.Println(err)
	}
	//上傳aes
	key = fileNames + "/image/" + fileNames + ".aes"
	localFile = aesDir + "/" + env + "/" + key
	err = bucket.PutObjectFromFile(key, localFile)
	if err != nil {
		fmt.Println(err)
	}
	//上傳key
	key = "key" + "/" + fileNames + "/image/" + fileNames + ".key"
	localFile = keyDir + "/" + env + "/" + fileNames + "/image/" + fileNames + ".key"
	err = bucket.PutObjectFromFile(key, localFile)
	if err != nil {
		fmt.Println(err)
	}

}

// 寫入videoKeyInfo
func writeVideoKeyInfo(keyInfoFileName string, keyfileName string, apiDomain string, id string) {
	f, err := os.Create(keyInfoFileName)
	if err != nil {
		fmt.Println(err)
	}
	defer f.Close()
	uri := "https://" + apiDomain + "/v3/v1/video/getvdokey?id=" + id
	f.Write([]byte(uri))
	f.Write([]byte("\n" + keyfileName))
}

// 取得所有apiDomain
func getApiDomain() []string {

	//建立db連線
	db := DbConnect()
	defer db.Close()

	sql := "select apiDomain from ApiDomain where isEnable = 1;"
	rows, err := db.Query(sql)
	if err != nil {
		fmt.Println(sql)
	}
	defer rows.Close()

	domains := []string{}
	domain := ""

	for rows.Next() {
		rows.Scan(&domain)
		domains = append(domains, domain[8:])
	}

	return domains

}

// 產生videoKey
func getRandomVideoKey() []byte {
	videoKey := make([]byte, 16)
	rand.Read(videoKey)
	return videoKey
}

// 寫入videoKey
func writeVideoKey(keyfileName string, videoKey []byte) {
	f, err := os.Create(keyfileName)
	if err != nil {
		fmt.Println(err)
	}
	defer f.Close()
	f.Write(videoKey)
}

// 取得影片路徑
func getVideoPath(queueFileName string) string {
	//開啟檔案
	inputFile, err := os.Open(queueFileName)
	if err != nil {
		fmt.Println(err)
	}
	defer inputFile.Close()

	//讀取
	counter := 0
	inputReader := bufio.NewReader(inputFile)
	for {
		inpiutString, err := inputReader.ReadString('\n')
		if err == io.EOF {
			return "找不到參數 ！ "
		}
		counter++
		if counter == 2 {

			if len(inpiutString) < 14 {
				return ""
			}

			return inpiutString[len(inpiutString)-31 : len(inpiutString)-1]
		}
	}
}

// 取得圖片路徑
func getImagePath(queueFileName string) string {
	//開啟檔案
	inputFile, err := os.Open(queueFileName)
	if err != nil {
		fmt.Println(err)
	}
	defer inputFile.Close()

	//讀取
	counter := 0
	inputReader := bufio.NewReader(inputFile)
	for {
		inpiutString, err := inputReader.ReadString('\n')
		if err == io.EOF {
			return "找不到參數 ！ "
		}
		counter++
		if counter == 3 {

			if len(inpiutString) < 14 {
				return ""
			}

			return inpiutString[len(inpiutString)-31 : len(inpiutString)-1]
		}
	}
}

// 圖片轉黨
func imageTransfer(fileNames string) {
	//讀檔
	fileName := imageDir + "/" + env + "/" + fileNames
	imageByte := readImage(fileName)

	//產Key
	imageKey := getRandomImageKey()
	imageIV := getRandomImageKey()

	//加密
	aesEncrypt := imageAes(imageByte, imageKey, imageIV)

	//建立資料夾
	mkdir(aesDir + "/" + env + "/" + fileNames[:len(fileNames)-19])
	mkdir(aesDir + "/" + env + "/" + fileNames[:len(fileNames)-19] + "/image/")
	mkdir(keyDir + "/" + env + "/" + fileNames[:len(fileNames)-19])
	mkdir(keyDir + "/" + env + "/" + fileNames[:len(fileNames)-19] + "/image/")

	//寫入擋案
	aesfileName := aesDir + "/" + env + "/" + fileNames[:len(fileNames)-19] + "/image/" + fileNames[:len(fileNames)-19] + ".aes"
	keyfileName := keyDir + "/" + env + "/" + fileNames[:len(fileNames)-19] + "/image/" + fileNames[:len(fileNames)-19] + ".key"
	writeAES(aesfileName, aesEncrypt)
	writeImageKey(keyfileName, imageKey, imageIV)

	//上傳騰訊雲
	uploadImage(fileNames[:len(fileNames)-19])

	//寫入db
	writeDB(fileNames, imageKey, imageIV)
}

// 建立資料夾
func mkdir(dirName string) {
	cmd := exec.Command("mkdir", dirName)
	cmd.Run()
}

// 寫入資料至db
func writeDB(fileNames string, imageKey string, imageIV string) {

	//建立db連線
	db := DbConnect()
	defer db.Close()

	//抓出id
	id := ""
	flag := false
	for i := range fileNames {
		if flag {
			if fileNames[i] != '-' {
				id += string(fileNames[i])
			} else {
				break
			}
		} else {
			if fileNames[i] == '-' {
				flag = true
			}
		}
	}

	//寫入db
	imageKey = "'" + imageKey + "'"
	imageIV = "'" + imageIV + "'"
	sql := fmt.Sprintf("update Videos set imageIV = %s, imageKey = %s, lastUpdateTime = NOW(), lastupdateUser = 'rd7' where id = %s;", imageIV, imageKey, id)
	rows, err := db.Query(sql)
	if err != nil {
		fmt.Println(sql)
	}
	defer rows.Close()

}

// 連線至資料庫
func DbConnect() *sql.DB {
	//建立連線
	var connectionString = fmt.Sprintf("%s:%s@tcp(%s:3306)/%s?allowNativePasswords=true", user, password, host, database)
	db, err := sql.Open("mysql", connectionString)

	if err != nil {
		fmt.Println(err)
	}

	return db
}

// 寫入imageKey
func writeImageKey(keyfileName string, imageKey string, imageIV string) {
	f, err := os.Create(keyfileName)
	if err != nil {
		fmt.Println(err)
	}
	defer f.Close()
	f.Write([]byte(imageKey + "\n" + imageIV))
}

// 寫入aes
func writeAES(aesfileName string, aesEncrypt []byte) {
	f, err := os.Create(aesfileName)
	if err != nil {
		fmt.Println(err)
	}
	defer f.Close()
	f.Write(aesEncrypt)
}

// 圖片加密
func imageAes(imageByte []byte, key string, iv string) []byte {
	keyByte, _ := hex.DecodeString(key)
	ivByte, _ := hex.DecodeString(iv)
	//補到block大小
	plaintext := PKCS7Padding(imageByte)
	ciphertext := make([]byte, len(plaintext))
	block, _ := aes.NewCipher(keyByte)
	mode := cipher.NewCBCEncrypter(block, ivByte)
	mode.CryptBlocks(ciphertext, plaintext)
	return ciphertext
}

// 補至block長度
func PKCS7Padding(ciphertext []byte) []byte {
	padding := aes.BlockSize - len(ciphertext)%aes.BlockSize
	padtext := bytes.Repeat([]byte{byte(padding)}, padding)
	return append(ciphertext, padtext...)
}

// 讀取圖片
func readImage(fileName string) []byte {

	f, _ := os.OpenFile(fileName, os.O_RDONLY, 0600)

	imageByte, _ := ioutil.ReadAll(f)

	return imageByte

}

// 產生imageKey
func getRandomImageKey() string {
	imageKey := ""
	elements := []rune{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'}
	for j := 0; j < 32; j++ {
		p, _ := rand.Int(rand.Reader, big.NewInt(int64(len(elements))))
		imageKey += string(elements[p.Int64()])
	}
	return imageKey
}

// 取得資料夾內完整檔名
func getFileNames(imageDir string) []string {
	originImage := []string{}
	files, _ := ioutil.ReadDir(imageDir)
	for i := range files {
		if i == 0 {
			continue
		}
		originImage = append(originImage, files[i].Name())
	}
	return originImage
}
