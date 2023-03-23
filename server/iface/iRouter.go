package iface

/*
	路由抽象接口
	路由里的苏局都是IRequest
*/

type IRouter interface {
	PreHandle(request IRequest)
	DoHandle(request IRequest)
	PostHandle(request IRequest)
}

// Handle 模版方法
func Handle(router IRouter, request IRequest) {
	router.PreHandle(request)
	router.DoHandle(request)
	router.PostHandle(request)
}
