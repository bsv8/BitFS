现在我要对客户端重新进行规划。

## 直接运行没有参数
- bitfs 在没有参数的情况下是信息显示。有效当前的公钥，余额以及费用池情况
- 一些私钥管理命令
- bitfs -import xxxxx 导入私钥
- bitfs -del 删除当前私钥
- bitfs -new 新创建一个私钥
- bitfs -status 这个命令就是什么都不带参数的一个别名。显示公钥,费用池，余额情况
- 私钥的管理秉存着安全的原则。如果有私钥导入和new都不能运行，必须显示的，删除后才能做new和import。

## 只带一个hex的就是下载种子以及文件
- 这个模式一般就是用户一次性下载文件，下载完后就结束的模式。
- bitfs 02bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb 这就是下载这个种子的文件
- 02bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb.bitfs 种子文件，下载到当前目录下
- filename.ext 正式文件,下载到当前网录下

## 后台进程模式
- 这个模式一般提供给卖方，在持续销售的同时，也可以购买一些文件。
- bitfs -d 运行在后台
- 这个模式主要通过HTTP API进行管理。