package config

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/mitchellh/mapstructure"
	"github.com/openimsdk/tools/errs"
	"github.com/openimsdk/tools/utils/runtimeenv"
	"github.com/spf13/viper"
)

func Load(configDirectory string, configFileName string, envPrefix string, config any) error {
	// [标准化部署] KUBERNETES 与 DOCKER 模式均优先从 CONFIG_PATH 挂载目录读取配置文件,
	// 以便 docker-compose 通过 volume 挂载固化后的配置, 不再依赖镜像内置配置或环境变量注入。
	// CONFIG_PATH 为空时回退到编译期 configDirectory(保持兼容)。
	// 注: 直接用字面量 "docker" 比较, 避免依赖 tools 版本是否导出 runtimeenv.Docker 常量
	if runtimeenv.RuntimeEnvironment() == KUBERNETES || runtimeenv.RuntimeEnvironment() == "docker" {
		mountPath := os.Getenv(MountConfigFilePath)
		if mountPath != "" {
			return loadConfig(filepath.Join(mountPath, configFileName), envPrefix, config)
		}
	}

	return loadConfig(filepath.Join(configDirectory, configFileName), envPrefix, config)
}

func loadConfig(path string, envPrefix string, config any) error {
	v := viper.New()
	v.SetConfigFile(path)
	v.SetEnvPrefix(envPrefix)
	v.AutomaticEnv()
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	if err := v.ReadInConfig(); err != nil {
		return errs.WrapMsg(err, "failed to read config file", "path", path, "envPrefix", envPrefix)
	}

	if err := v.Unmarshal(config, func(config *mapstructure.DecoderConfig) {
		config.TagName = StructTagName
	}); err != nil {
		return errs.WrapMsg(err, "failed to unmarshal config", "path", path, "envPrefix", envPrefix)
	}
	return nil
}
