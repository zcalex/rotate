package rotate


import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	backupTimeFormat   = "2006-01-02T15-04-05.000"
	defaultFileMaxSize = 100
)

const (
	ConstRotateHour     = iota // 按每小时输出
	ConstRotateFileSize        // 按照文件大小rotate
)

var _ io.WriteCloser = (*Logger)(nil)

type Logger struct {
	// 文件路径
	Dir string `json:"dir" yaml:"dir" xml:"dir"`
	// 文件名
	FileName string `json:"filename" yaml:"file_name" xml:"filename"`
	// 单条日志最大长度
	LogMaxSize int64 `json:"log_max_size" yaml:"log_max_size" xml:"log_max_size"`
	// 刷新规则
	RotateType int64 `json:"rotate_type" yaml:"rotate_type" xml:"rotate_type"`
	// 文件最大长度
	// 只在constRotateFileSize下生效
	FileMaxSize int64 `json:"maxsize" yaml:"file_max_size" xml:"file_max_size"`
	// 文件过期时间
	// constRotateFileSize模式下对文件创建时间
	// constRotateHour模式下是文件加创建时间
	MaxAge int64 `json:"max_age" yaml:"max_age" xml:"max_age"`
	// 文件最大备份数量
	// constRotateFileSize模式下生效
	MaxBackups int64 `json:"max_backups" yaml:"max_backups" xml:"max_backups"`

	// 是否缓存
	Cache bool `json:"cache" yaml:"cache" xml:"cache"`

	// 用于记录文件大小，只在constRotateFileSize下生效
	size int64
	// file
	file *os.File
	// 锁
	mu sync.Mutex
	// 下次日志刷新时间，constRotateHour模式下有效
	nextRotateTime time.Time

	// 是否是本地时间
	LocalTime bool `json:"localtime" yaml:"localtime" xml:"localtime"`

	// todo 后期可以优化， 缓存
	cache []byte

	millCh    chan bool
	startMill sync.Once

	prefix string
	ext    string
}

var (
	currentTime = time.Now
	os_Stat     = os.Stat
	megabyte    = 1024 * 1024
)

func stack() string {
	_, file, line, ok := runtime.Caller(6)
	if ok {
		return file + ":" + strconv.Itoa(line)
	}

	return "no stack"
}

func (l *Logger) Write(p []byte) (n int, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	writeLen := int64(len(p))
	// 单条记录不能太长
	if writeLen > l.logMax() {
		// 如果记录太长，把栈打出来
		return 0, fmt.Errorf(
			"write length %d exceeds maximum record size %d  stack:%v", writeLen, l.logMax(), stack(),
		)
	}

	if l.file == nil {
		if err = l.openExistingOrNew(int64(len(p))); err != nil {
			return 0, err
		}
	}

	// 检查是否可以刷新
	if l.rotateEnable(int64(len(p))) {
		if err := l.rotate(); err != nil {
			return 0, err
		}
	}

	n, err = l.file.Write(p)
	l.size += int64(n)
	return n, err
}

// Close implements io.Closer, and closes the current logfile.
func (l *Logger) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.close()
}

// close closes the file if it is open.
func (l *Logger) close() error {
	if l.file == nil {
		return nil
	}
	err := l.file.Close()
	l.file = nil
	return err
}

// 是否可以rotate
func (l *Logger) rotateEnable(writeLen int64) bool {
	switch l.RotateType {
	case ConstRotateHour:
		return time.Now().UnixNano() >= l.nextRotateTime.UnixNano()
	default:
		return l.size+writeLen > l.fileMax()
	}
}

func (l *Logger) Rotate() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.rotate()
}

func (l *Logger) rotate() error {
	// 关闭旧文件，打开新文件
	if err := l.close(); err != nil {
		return err
	}
	if err := l.openNew(); err != nil {
		return err
	}

	l.mill()
	return nil
}

func (l *Logger) openNew() error {
	// 目录
	err := os.MkdirAll(l.dir(), 0744)
	if err != nil {
		return fmt.Errorf("can't make directories for new logfile: %s", err)
	}

	// 文件
	name := l.filename()
	mode := os.FileMode(0644)
	info, err := os_Stat(name)
	// 对于ConstRotateFileSize类型需要数据备份，且必须MaxBackups > 1
	if err == nil && l.RotateType == ConstRotateFileSize && l.MaxBackups > 0 {
		mode = info.Mode()
		newName := backupName(name, l.LocalTime)
		if err := os.Rename(name, newName); err != nil {
			return fmt.Errorf("can't rename log file: %s", err)
		}

		if err := chown(name, info); err != nil {
			return err
		}
	}

	// 创建新文件
	f, err := os.OpenFile(name, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
	if err != nil {
		return fmt.Errorf("can't open new logfile: %s", err)
	}
	l.file = f
	l.size = 0
	l.nextRotateTime = getNextRotateTime()
	return nil
}

func backupName(name string, local bool) string {
	dir := filepath.Dir(name)
	filename := filepath.Base(name)
	ext := filepath.Ext(filename)
	prefix := filename[:len(filename)-len(ext)]
	t := currentTime()
	if !local {
		t = t.UTC()
	}

	timestamp := t.Format(backupTimeFormat)
	return filepath.Join(dir, fmt.Sprintf("%s-%s%s", prefix, timestamp, ext))
}

func (l *Logger) openExistingOrNew(writeLen int64) error {
	l.mill()

	// 获得日志名的前缀以及后缀
	logFileName := filepath.Base(l.baseFileName())
	l.ext = filepath.Ext(logFileName)
	l.prefix = logFileName[:len(logFileName)-len(l.ext)]

	filename := l.filename()
	info, err := os_Stat(filename)
	if os.IsNotExist(err) {
		return l.openNew()
	}
	if err != nil {
		return fmt.Errorf("error getting log file info: %s", err)
	}

	// 只有在文件rolling的情况下才需要判断文件大小
	if l.RotateType != ConstRotateHour {
		if info.Size()+writeLen >= l.fileMax() {
			return l.rotate()
		}
	}

	file, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return l.openNew()
	}
	l.file = file
	l.size = info.Size()
	l.nextRotateTime = getNextRotateTime()
	return nil
}

func (l *Logger) millRunOnce() error {
	if l.MaxBackups == 0 && l.MaxAge == 0 {
		return nil
	}

	// 所有的日志
	files, err := l.oldLogFiles()
	if err != nil {
		return err
	}

	var remove []logInfo
	if l.GetRotateType() == ConstRotateFileSize && l.MaxBackups > 0 && l.MaxBackups < int64(len(files)) {
		remove = append(remove, files[l.MaxBackups:]...)
		files = files[0:l.MaxBackups]
	}

	if l.MaxAge > 0 {
		diff := time.Duration(int64(24*time.Hour) * int64(l.MaxAge))
		cutoff := currentTime().Add(-1 * diff)

		var remaining []logInfo
		for _, f := range files {
			if f.timestamp.Before(cutoff) {
				remove = append(remove, f)
			} else {
				remaining = append(remaining, f)
			}
		}
		files = remaining
	}

	for _, f := range remove {
		errRemove := os.RemoveAll(filepath.Join(l.Dir, f.Name()))
		if err == nil && errRemove != nil {
			err = errRemove
		}
	}

	return err
}

func (l *Logger) millRun() {
	for _ = range l.millCh {
		_ = l.millRunOnce()
	}
}

func (l *Logger) mill() {
	l.startMill.Do(func() {
		l.millCh = make(chan bool, 1)
		go l.millRun()
	})
	select {
	case l.millCh <- true:
	default:
	}
}

func (l *Logger) oldLogFiles() ([]logInfo, error) {
	files, err := ioutil.ReadDir(l.Dir)
	if err != nil {
		return nil, fmt.Errorf("can't read log file directory: %s", err)
	}
	var logFiles []logInfo

	for _, f := range files {
		if l.GetRotateType() == ConstRotateHour && l.IsLogDir(f) {
			logFiles = append(logFiles, logInfo{f.ModTime(), f})
			continue
		} else if l.IsLogFile(f) {
			logFiles = append(logFiles, logInfo{f.ModTime(), f})
			continue
		}
	}

	sort.Sort(byFormatTime(logFiles))
	return logFiles, nil
}

// 基础接口
func (l *Logger) logMax() int64 {
	if l.LogMaxSize != 0 {
		return int64(l.LogMaxSize)
	}
	return int64(megabyte)

}

func (l *Logger) fileMax() int64 {
	if l.FileMaxSize != 0 {
		return l.FileMaxSize * int64(megabyte)
	}

	return l.FileMaxSize * defaultFileMaxSize
}

// 文件名
func (l *Logger) filename() string {
	prefix, ext := l.GetFilePrefix(), l.GetFileExt()
	switch l.RotateType {
	case ConstRotateHour:
		tm := currentTime()
		tmStr := fmt.Sprintf("%04d%02d%02d_%02d", tm.Year(), tm.Month(), tm.Day(), tm.Hour())
		dailyStr := fmt.Sprintf("%04d%02d%02d", tm.Year(), tm.Month(), tm.Day())
		return path.Join(l.Dir, dailyStr, fmt.Sprintf("%s_%s%s", prefix, tmStr, ext))
	default:
		return path.Join(l.Dir, l.baseFileName())
	}
}

func (l *Logger) baseFileName() string {
	if l.FileName == "" {
		return "logger.log"
	}
	return l.FileName
}

// 文件路径
func (l *Logger) dir() string {
	switch l.RotateType {
	case ConstRotateHour:
		tm := currentTime()
		suffix := fmt.Sprintf("%04d%02d%02d", tm.Year(), tm.Month(), tm.Day())
		return path.Join(l.Dir, suffix)
	default:
		return l.Dir
	}
}

func (l *Logger) GetFilePrefix() string {
	return l.prefix
}

func (l *Logger) GetFileExt() string {
	return l.ext
}

func (l *Logger) GetRotateType() int64 {
	return l.RotateType
}

func (l *Logger) IsLogDir(d os.FileInfo) bool {
	if !d.IsDir() {
		return false
	}

	_, err := strconv.ParseInt(d.Name(), 10, 64)
	if err != nil && len(d.Name()) != 8 {
		return false
	}
	return true
}

func (l *Logger) IsLogFile(f os.FileInfo) bool {
	if f.IsDir() {
		return false
	}

	prefix := l.GetFilePrefix()
	ext := l.GetFileExt()
	fileName := f.Name()

	if !strings.HasPrefix(fileName, prefix+"-") {
		return false
	}
	if !strings.HasSuffix(fileName, ext) {
		return false
	}
	return true
}

func getNextRotateTime() time.Time {
	tm := currentTime()
	currHour := time.Date(tm.Year(), tm.Month(), tm.Day(), tm.Hour(), 0, 0, 0, time.Local)
	nextHour := currHour.Add(time.Hour)
	return nextHour
}

// logInfo数据 用于删除旧数据
type logInfo struct {
	timestamp time.Time
	os.FileInfo
}

type byFormatTime []logInfo

func (b byFormatTime) Less(i, j int) bool {
	return b[i].timestamp.After(b[j].timestamp)
}

func (b byFormatTime) Swap(i, j int) {
	b[i], b[j] = b[j], b[i]
}

func (b byFormatTime) Len() int {
	return len(b)
}
