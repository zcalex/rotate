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
	backupTimeFormat   = "04_05.000"
	defaultFileMaxSize = 100
	fileNameExt        = ".log"
	interval           = time.Hour
	megabyte           = 1024 * 1024
)

var _ io.WriteCloser = (*Logger)(nil)
var defaultOutput = os.Stdout

type Logger struct {
	// 文件路径
	Dir string `json:"dir" yaml:"dir" xml:"dir"`
	// 文件名
	FileName string `json:"filename" yaml:"file_name" xml:"filename"`
	// 单条日志最大长度
	LogMaxSize uint64 `json:"log_max_size" yaml:"log_max_size" xml:"log_max_size"`
	// 单个文件的最大size
	FileMaxSize uint64 `json:"maxsize" yaml:"file_max_size" xml:"file_max_size"`
	// 文件过期时间
	MaxAge uint64 `json:"max_age" yaml:"max_age" xml:"max_age"`
	// 是否缓存,
	// todo 暂未支持
	Cache     bool  `json:"cache" yaml:"cache" xml:"cache"`
	CacheSize int32 `json:"cache_size" yaml:"cache_size" xml:"cache_size"`

	// 用于记录文件大小
	size uint64
	// 输出
	writer io.WriteCloser
	// 是否是文件，标准输出不需要关闭
	isFile bool
	// 锁
	mu sync.Mutex
	// 下次日志刷新时间，constRotateHour模式下有效
	nextRotateTime time.Time
	// todo 后期可以优化， 缓存
	cache []byte

	millCh    chan bool
	startMill sync.Once
}

var (
	currentTime = time.Now
	os_Stat     = os.Stat
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

	writeLen := uint64(len(p))
	// 单条记录不能太长
	if writeLen > l.logMax() {
		// 如果记录太长，把栈打出来
		return 0, fmt.Errorf(
			"write length %d exceeds maximum record size %d  stack:%v", writeLen, l.logMax(), stack(),
		)
	}

	if l.writer == nil {
		if err = l.openExistingOrNew(uint64(len(p))); err != nil {
			return 0, err
		}
	}

	// 检查是否可以刷新
	if l.rotateEnable(uint64(len(p))) {
		if err := l.rotate(); err != nil {
			return 0, err
		}
	}

	n, err = l.writer.Write(p)
	l.size += uint64(n)
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
	if l.writer == nil || !l.isFile {
		return nil
	}
	err := l.writer.Close()
	l.writer = nil
	return err
}

// 是否可以rotate
func (l *Logger) rotateEnable(writeLen uint64) bool {
	return time.Now().UnixNano() >= l.nextRotateTime.UnixNano() || l.size+writeLen > l.fileMax()
}

func (l *Logger) Rotate() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.rotate()
}

func (l *Logger) emptyFile() bool {
	return l.FileName == ""
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
	// 如果文件为空，输出到控制台
	if l.emptyFile() {
		l.writer = defaultOutput
		return nil
	}

	// 目录
	err := os.MkdirAll(l.dir(), 0744)
	if err != nil {
		return fmt.Errorf("can't make directories for new logfile: %s", err)
	}

	// 文件
	name := l.filename()
	mode := os.FileMode(0644)
	info, err := os_Stat(name)
	// 备份日志
	if err == nil {
		mode = info.Mode()
		// 备份一个小时内的多份日志文件
		newName := backupName(name)
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
	l.writer = f
	l.isFile = true
	l.size = 0
	l.nextRotateTime = getNextRotateTime()
	return nil
}

func backupName(name string) string {
	dir := filepath.Dir(name)
	filename := filepath.Base(name)
	t := currentTime()

	timestamp := t.Format(backupTimeFormat)
	return filepath.Join(dir, filename+"-"+timestamp)
}

func (l *Logger) openExistingOrNew(writeLen uint64) error {
	// 如果没有配置输出文件名，直接输出到控制台
	if l.emptyFile() {
		l.writer = defaultOutput
		return nil
	}

	l.mill()
	filename := l.filename()
	info, err := os_Stat(filename)
	if os.IsNotExist(err) {
		return l.openNew()
	}
	if err != nil {
		return fmt.Errorf("error getting log file info: %s", err)
	}

	// 只有在文件rolling的情况下才需要判断文件大小
	if uint64(info.Size())+writeLen >= l.fileMax() {
		return l.rotate()
	}

	file, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return l.openNew()
	}
	l.writer = file
	l.isFile = true
	l.size = uint64(info.Size())
	l.nextRotateTime = getNextRotateTime()
	return nil
}

func (l *Logger) millRunOnce() error {
	if l.MaxAge == 0 {
		return nil
	}

	// 所有的日志的文件夹
	files, err := l.oldLogFiles()
	if err != nil {
		return err
	}
	var remove []logInfo
	if l.MaxAge > 0 {
		diff := time.Duration(int64(24*time.Hour) * int64(l.MaxAge))
		cutoff := currentTime().Add(-1 * diff)
		for _, f := range files {
			if f.timestamp.Before(cutoff) {
				remove = append(remove, f)
			}
		}
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
		if l.IsLogDir(f) {
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
func (l *Logger) logMax() uint64 {
	if l.LogMaxSize != 0 {
		return uint64(l.LogMaxSize)
	}
	return uint64(megabyte)

}

func (l *Logger) fileMax() uint64 {
	if l.FileMaxSize != 0 {
		return l.FileMaxSize * uint64(megabyte)
	}

	return defaultFileMaxSize * uint64(megabyte)
}

// 文件名
func (l *Logger) filename() string {
	prefix, ext := l.baseFileName(), fileNameExt
	tm := currentTime()
	tmStr := fmt.Sprintf("%04d%02d%02d_%02d", tm.Year(), tm.Month(), tm.Day(), tm.Hour())
	dailyStr := fmt.Sprintf("%04d%02d%02d", tm.Year(), tm.Month(), tm.Day())
	return path.Join(l.Dir, dailyStr, fmt.Sprintf("%s_%s%s", prefix, tmStr, ext))
}

func (l *Logger) baseFileName() string {
	return l.FileName
}

// 文件路径
func (l *Logger) dir() string {
	tm := currentTime()
	suffix := fmt.Sprintf("%04d%02d%02d", tm.Year(), tm.Month(), tm.Day())
	return path.Join(l.Dir, suffix)
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

	prefix := l.baseFileName()
	fileName := f.Name()

	if !strings.HasPrefix(fileName, prefix) {
		return false
	}
	if !strings.HasSuffix(fileName, fileNameExt) {
		return false
	}
	return true
}

func getNextRotateTime() time.Time {
	return currentTime().Truncate(interval).Add(interval)
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
