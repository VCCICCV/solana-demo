# 一个使用yellowstone-grpc的例子

## Windows请使用WSL

测试端口连通性

安装grpcurl

```cmd
# Linux (Debian/Ubuntu)
sudo apt update && sudo apt install -y grpcurl

# Mac (Homebrew)
brew install grpcurl
```

测试

```cmd
grpcurl -plaintext solana-yellowstone-grpc.publicnode.com:443 list
```
