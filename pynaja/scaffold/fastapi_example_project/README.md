# fastapi_example_project



## 项目层级结构

```tex
├── fastapi_example_project                     项目目录
│    ├── abc_base                               项目抽象基类层
│    │    ├── model                           	抽象模型层
│    │    ├── service                           抽象服务层
│    │    └── view                          	抽象视图层
│    ├── apps                                   项目应用目录
│    │    ├── example_app                       单一应用
│    │ 	  │  	├── sql_logs                    该应用sql修改记录
│    │ 	  │  	├── enum.py                     该应用枚举类常量
│    │ 	  │  	├── model.py                    该应用模型文件
│    │ 	  │ 	├── service.py                  该应用服务文件
│    │    │  	└── view.py                     该应用视图文件	
│    ├── utils                                  项目工具层目录
│    │    └── response                          自定义响应
│    ├── conf                                   项目配置层目录
│    │    ├── dynamic.conf                      本地动态配置文件
│    │    ├── dynamic.dev.conf                  dev环境动态配置文件
│    │    ├── dynamic.rel.conf                  生产环境动态配置文件
│    │    ├── static.conf                       本地静态配置文件
│    │    ├── static.dev.conf                   dev环境静态配置文件
│    │    ├── static.rel.conf                   生产环境静态配置文件
│    │    └── gunicorn.conf                     gunicorn配置文件
│    ├── services                               项目独立/三方服务目录
│    ├── main.py                                fastapi实例对象入口
└────└── router.py                              项目路由集
```




## 本地启动

```shell
pip install pynaja

// 完成本地动态和静态配置文件
cd wechat-service-be/
mv wechat/conf/dynamic.dev.conf wechat/conf/dynamic.conf
mv wechat/conf/static.dev.conf wechat/conf/static.conf

vi wechat/conf/dynamic.conf
vi wechat/conf/static.conf


// 使用uvicorn启动项目
uvicorn fastapi_example_project.main:app --reload --port 8000
```



## 接口文档查看

```tex
swagger UI, http://127.0.0.1:8000/docs
redoc url, http://127.0.0.1:8000/redoc
```
