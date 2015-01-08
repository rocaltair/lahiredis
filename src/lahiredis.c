/*
 * Copyright (c) 2014, Roc
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 * * Redistributions of source code must retain the above copyright notice, this
 *   list of conditions and the following disclaimer.
 * 
 * * Redistributions in binary form must reproduce the above copyright notice,
 *   this list of conditions and the following disclaimer in the documentation
 *   and/or other materials provided with the distribution.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

/*
 * Author : Zhu Peng
 * E-mail : rocaltair@gmail.com
 */

#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <lua.h>
#include <lauxlib.h>
#include <hiredis/hiredis.h>
#include <hiredis/async.h>
#include <assert.h>

/*
 * #define LAHIREDIS_ENABLE_REDIS_DEBUG 1
 */
/*
* #define LAHIREDIS_BACKEND 0
*/

#ifdef LAHIREDIS_ENABLE_REDIS_DEBUG
#  define LAHIREDIS_DLOG(fmt, args...) fprintf(stderr, fmt "\n", ##args)
#else
#  define LAHIREDIS_DLOG(fmt, args...)
#endif

#define LAHIREDIS_BACKEND_LIBEV 0
#define	LAHIREDIS_BACKEND_LIBEVENT 1


#ifdef LAHIREDIS_BACKEND
#  if (LAHIREDIS_BACKEND == LAHIREDIS_BACKEND_LIBEV)
#    include <ev.h>
#    include <hiredis/adapters/libev.h>
#  elif (LAHIREDIS_BACKEND == LAHIREDIS_BACKEND_LIBEVENT)
#    include <event2/event.h>
#    include <hiredis/adapters/libevent.h>
#  else
#    error "LAHIREDIS_BACKEND not defined! 0  for LAHIREDIS_BACKEND_LIBEV, and 1 for LAHIREDIS_BACKEND_LIBEVENT"
#  endif
#else
#  error "LAHIREDIS_BACKEND not defined!  0 for LAHIREDIS_BACKEND_LIBEV, and 1 for LAHIREDIS_BACKEND_LIBEVENT"
#endif

#define LIBNAME_LAHIREDIS "lahiredis"
#define LIB_HIREDIS_VERSION 0.10

#define LAHIREDIS_SET_CONST_INTEGER(L, name, value) \
	lua_pushliteral(L, #name);\
	lua_pushinteger(L, value);\
	lua_settable(L, -3);

#define LAHIREDIS_SET_CONST_STR(L, name, value) \
	lua_pushliteral(L, #name);\
	lua_pushstring(L, value);\
	lua_settable(L, -3);

#define LAHIREDIS_CONN_CLASS "lahiredis{conn}"
#define LAHIREDIS_EVENT_BASE_CLASS "lahiredis{event_base}"

#if LUA_VERSION_NUM < 502
#  define luaL_newlib(L,l) (lua_newtable(L), luaL_register(L,NULL,l))
#  ifndef LUA_RIDX_MAINTHREAD 
#    define LUA_RIDX_MAINTHREAD	1
#  endif
#endif

#define LAHIREDIS_MAXARGS 256
/*#define LAHIREDIS_ENABLE_UNREF 1*/

#define check_redis_conn(l, idx) \
	*(redisAsyncContext **)luaL_checkudata(l, idx, LAHIREDIS_CONN_CLASS)


#ifndef LAHIREDIS_LUA_SAFE_CALL
#  define LAHIREDIS_LUA_SAFE_CALL lua_safe_call
#endif

#define LAHIREDIS_IS_CONN_CONNECTED(conn)\
	(((redisContext *)(conn))->flags & REDIS_CONNECTED)

typedef enum {
	LAHIREDIS_CB_TYPE_CONNECT,
	LAHIREDIS_CB_TYPE_DISCONNECT,
} cb_type_t;

typedef struct lahiredis_aysnc_conn_t {
	lua_State *L;
	int L_ref;
	int disconnect_cb_idx;
	int connect_cb_idx;
} lahiredis_aysnc_conn_t;

static lua_State* get_main_thread(lua_State *L)
{
	lua_rawgeti(L,  LUA_REGISTRYINDEX, LUA_RIDX_MAINTHREAD);
	lua_State *mL = lua_tothread(L, -1);
	lua_pop(L, 1);
	return mL;
}

#define CONN_CHECK_VALID(L, c)				\
	do {						\
		if( c == NULL){			\
			luaL_error(L, "valid conn!");	\
			lua_settop(L, 0);		\
			return 0;			\
		}					\
	} while (0)

static int traceback (lua_State *L)
{
        lua_getfield(L, LUA_GLOBALSINDEX, "debug");
        if (!lua_istable(L, -1))
        {
                lua_pop(L, 1);
                return 1;
        }
        lua_getfield(L, -1, "excepthook");
        if (!lua_isfunction(L, -1))
        {
                lua_pop(L, 2);
                return 1;
        }
        lua_pushvalue(L, 1);  //pass error message
        lua_pcall(L, 1, 1, 0);  //call debug.traceback
        return 1;
}

static int lua_safe_call (
		lua_State *L,
		int narg,
		int nresults,
		int err_handle /*non-use*/
		)
{
        int status;
        int base = lua_gettop(L) - narg;  //function index
        lua_pushcfunction(L, traceback);  //push traceback function
        lua_insert(L, base);  //put it under chunk and args
        status = lua_pcall(L, narg, nresults, base);
        lua_remove(L, base);  //remove traceback function
        return status;
}

static void disconnect_cb(const redisAsyncContext *c, int status)
{
	lahiredis_aysnc_conn_t *conn = (lahiredis_aysnc_conn_t *)c->data;
	LAHIREDIS_DLOG("async_conn_gc is called,state=%p,c=%p,status=%d", conn->L, c, status);
	lua_State *L = get_main_thread(conn->L);
	lua_getref(L, conn->disconnect_cb_idx);
	lua_pushinteger(L, status);
	LAHIREDIS_LUA_SAFE_CALL(L, 1, 0, 0);
#ifdef LAHIREDIS_ENABLE_UNREF
	lua_unref(L, conn->disconnect_cb_idx);
	lua_unref(L, conn->L_ref);
#endif
	free(c->data);
}

static void connect_cb(const redisAsyncContext *c, int status)
{
	lahiredis_aysnc_conn_t *conn = (lahiredis_aysnc_conn_t *)c->data;
	/*use main thread here while invoked in coroutine*/
	lua_State *L = get_main_thread(conn->L);
	lua_getref(L, conn->connect_cb_idx);
	lua_pushinteger(L, status);
	/*can not free here*/
	LAHIREDIS_LUA_SAFE_CALL(L, 1, 0, 0);
#ifdef LAHIREDIS_ENABLE_UNREF
	lua_unref(L, conn->connect_cb_idx);
#endif
	if (status != REDIS_OK) {
		free(c->data);
	}
}

static int async_set_cb(lua_State *L, cb_type_t cb_type)
{
	int cb_idx;
	redisAsyncContext *c = check_redis_conn(L, 1);
	CONN_CHECK_VALID(L, c);
	if (!lua_isfunction(L, 2)) {
		lua_pushfstring(L, "function args #2 in %s,cb function required!", __FUNCTION__);
		lua_error(L);
		return 0;
	}
	cb_idx = lua_ref(L, 1);
	lahiredis_aysnc_conn_t *conn = (lahiredis_aysnc_conn_t *)c->data;
	LAHIREDIS_DLOG("async_set_cb %d", cb_idx);
	switch (cb_type) {
		case LAHIREDIS_CB_TYPE_CONNECT: {
				conn->connect_cb_idx = cb_idx;
				redisAsyncSetConnectCallback(c, connect_cb);
				LAHIREDIS_DLOG("connect cb set %d", cb_idx);
			}
			break;
		case LAHIREDIS_CB_TYPE_DISCONNECT: {
				conn->disconnect_cb_idx = cb_idx;
				redisAsyncSetDisconnectCallback(c, disconnect_cb);
				LAHIREDIS_DLOG("disconnect cb set %d", cb_idx);
			}
			break;
		default:
			break;
	}
	return 0;
}

static int async_conn_set_connect_cb(lua_State *L)
{
	return async_set_cb(L, LAHIREDIS_CB_TYPE_CONNECT);
}

static int async_conn_set_disconnect_cb(lua_State *L)
{
	return async_set_cb(L, LAHIREDIS_CB_TYPE_DISCONNECT);
}

#if (LAHIREDIS_BACKEND == LAHIREDIS_BACKEND_LIBEVENT)
int set_event_base(lua_State *L, struct event *base, const char *base_event_name)
{
	*(redisAsyncContext **)lua_newuserdata(L, sizeof(void *)) = base;	/*would never be free*/
	lua_setglobal(L, base_event_name);
	return 0;
}
#endif

static int async_conn_connect(lua_State *L)
{
	redisAsyncContext *c = check_redis_conn(L, 1);
	CONN_CHECK_VALID(L, c);
#if (LAHIREDIS_BACKEND == LAHIREDIS_BACKEND_LIBEV)
	redisLibevAttach(EV_DEFAULT_ c);
	LAHIREDIS_DLOG("attach to libev");
#elif (LAHIREDIS_BACKEND == LAHIREDIS_BACKEND_LIBEVENT)
	/* TODO get base event*/
	const char *base_event_name = luaL_checkstring(L, 2);
	struct event *base = NULL;
	/*
	*(redisAsyncContext **)luaL_checkudata(l, idx, LAHIREDIS_EVENT_BASE_CLASS)
	*/
	redisLibeventAttach(c, base);
	LAHIREDIS_DLOG("attach to libevent");
#endif
	return 0;
}

static int parse_reply(lua_State *L, redisReply *r)
{
	int i = 0;
	int args_size = 0;
	int top_begin = lua_gettop(L);
	LAHIREDIS_DLOG("reply type=%d", r->type);
	switch (r->type) {
		case REDIS_REPLY_STRING :
			lua_pushlstring(L, r->str, r->len);
			break;
		case REDIS_REPLY_ARRAY: {
				lua_newtable(L);
				for (i = 0; i < r->elements; i++) {
					lua_pushinteger(L, i+1);
					parse_reply(L, r->element[i]);
					lua_rawset(L, -3);
				}
			}
			break;
		case REDIS_REPLY_INTEGER :
			lua_pushnumber(L, (lua_Number)r->integer);
			break;
		case REDIS_REPLY_NIL :
			lua_pushnil(L);
			break;
		case REDIS_REPLY_STATUS: {
				int ok = strcmp("OK", r->str) == 0;
				lua_pushboolean(L, ok);
			}
			break;
		case REDIS_REPLY_ERROR: {
				LAHIREDIS_DLOG("reply error!err=%s", r->str);
				lua_pushnil(L);
				lua_pushstring(L, r->str);
			}
			break;
		default:
			break;
	}
	args_size = lua_gettop(L) - top_begin;
	LAHIREDIS_DLOG("reply cb nargs=%d", args_size);
	return args_size;
}

static void run_cmd_cb(
		redisAsyncContext *c,
		void *r,
		void *privdata
		)
{
	int args_size = 0;
	redisReply *reply = (redisReply *)r;
	int exec_cb_idx = (intptr_t)privdata;
	lahiredis_aysnc_conn_t *conn = (lahiredis_aysnc_conn_t *)c->data;
	lua_State *L = get_main_thread(conn->L);
	lua_getref(L, exec_cb_idx);
	/*
	 * parse reply
	 */
	if (r == NULL) {
		lua_pushinteger(L, REDIS_REPLY_ERROR);
		lua_pushnil(L);
		lua_pushstring(L, "ERR get reply failed!");
		args_size = 3;
	} else {
		lua_pushinteger(L, reply->type);
		args_size = parse_reply(L, reply) + 1;
	}
	
	LAHIREDIS_LUA_SAFE_CALL(L, args_size, 0, 0);
#ifdef LAHIREDIS_ENABLE_UNREF
	lua_unref(L, exec_cb_idx);
#endif
}

static int load_args(lua_State * L,
		int idx, /* index of first argument */
		const char ** argv,
		size_t * argvlen
		)
{
	int i = 0;
	int nargs = lua_gettop(L) - idx + 1;
	if (nargs <= 0) {
		return luaL_error(L, "missing command name");
	}
	if (nargs > LAHIREDIS_MAXARGS) {
		return luaL_error(L, "too many arguments");
	}
	for (i = 0; i < nargs; ++i) {
		size_t len = 0;
		const char * str = lua_tolstring(L, idx + i, &len);
		if (str == NULL) {
			return luaL_argerror(L, idx + i, "expected a string or number value");
		}
		argv[i] = str;
		argvlen[i] = len;
	}
	return nargs;
}

static int async_conn_execute(lua_State *L)
{
	int nargs;
	int exec_cb_idx;
	int ret_status;
	const char *argv[LAHIREDIS_MAXARGS];
	size_t argvlen[LAHIREDIS_MAXARGS];
	redisAsyncContext *c = check_redis_conn(L, 1);
	CONN_CHECK_VALID(L, c);
	if (!lua_isfunction(L, 2)) {
		lua_pushfstring(L, "function args #(end) in %s, exec_cb function required!", __FUNCTION__);
		lua_error(L);
		return 0;
	}
	nargs = load_args(L, 3, argv, argvlen);
	lua_settop(L, 2);
	exec_cb_idx = lua_ref(L, 1);
	LAHIREDIS_DLOG("execute args=%d", nargs);
	if (!LAHIREDIS_IS_CONN_CONNECTED(c)) {
		lua_settop(L, 0);
		lua_getref(L, exec_cb_idx);
		lua_pushinteger(L, REDIS_REPLY_ERROR);
		lua_pushnil(L);
		lua_pushfstring(L, "cannot connect to redis-server,LINE=%s", __LINE__);
		LAHIREDIS_LUA_SAFE_CALL(L, 3, 0, 0);
#ifdef LAHIREDIS_ENABLE_UNREF
		lua_unref(L, exec_cb_idx);
#endif
		lua_pushinteger(L, REDIS_REPLY_ERROR);
		lua_pushfstring(L, "cannot connect to redis-server,LINE=%s", __LINE__);
		return 2;
	}
	ret_status = redisAsyncCommandArgv(c, run_cmd_cb, (void *)((intptr_t)exec_cb_idx), nargs, argv, argvlen);
	lua_pushinteger(L, ret_status);
	return 1;
}

static int async_conn_execute_str(lua_State *L)
{
	int exec_cb_idx;
	int ret_status;
	size_t size;
	redisAsyncContext *c = check_redis_conn(L, 1);
	CONN_CHECK_VALID(L, c);
	if (!lua_isfunction(L, 2)) {
		lua_pushfstring(L, "function args #3 in %s, exec_cb function required!", __FUNCTION__);
		lua_error(L);
		return 0;
	}
	const char *fmted = luaL_checklstring(L, 3, &size);
	if (NULL == fmted || strchr(fmted, '%')) {
		luaL_error(L, "command format err in execs");
		lua_settop(L, 0);
		return 0;
	}
	lua_settop(L, 2);
	exec_cb_idx = lua_ref(L, 1);
	if (!LAHIREDIS_IS_CONN_CONNECTED(c)) {
		lua_settop(L, 0);
		lua_getref(L, exec_cb_idx);
		lua_pushinteger(L, REDIS_REPLY_ERROR);
		lua_pushnil(L);
		lua_pushfstring(L, "cannot connect to redis-server,LINE=%s", __LINE__);
		LAHIREDIS_LUA_SAFE_CALL(L, 3, 0, 0);
#ifdef LAHIREDIS_ENABLE_UNREF
		lua_unref(L, exec_cb_idx);
#endif

		lua_pushinteger(L, REDIS_REPLY_ERROR);
		lua_pushfstring(L, "cannot connect to redis-server,LINE=%s", __LINE__);
		return 2;
	}
	ret_status = redisAsyncCommand(c, run_cmd_cb, (void *)((intptr_t)exec_cb_idx), fmted);
	lua_pushinteger(L, ret_status);
	return 1;
}

static int async_conn_is_connected(lua_State *L)
{
	redisAsyncContext *c = check_redis_conn(L, 1);
	CONN_CHECK_VALID(L, c);
	lua_pushboolean(L, LAHIREDIS_IS_CONN_CONNECTED(c));
	return 1;
}

static int async_conn_disconnect(lua_State *L)
{
	redisAsyncContext **ud = (redisAsyncContext **)luaL_checkudata(L, 1, LAHIREDIS_CONN_CLASS);
	redisAsyncContext *c = *ud;
	*ud = NULL;
	CONN_CHECK_VALID(L, c);
	LAHIREDIS_DLOG("async_conn_disconnect is called,state=%p", L);
	if (LAHIREDIS_IS_CONN_CONNECTED(c)) {
		redisAsyncDisconnect(c);
	} else {
		LAHIREDIS_DLOG("%p may be leaked???", c);
	}
	return 0;
	/* TODO a bug here */
}

static luaL_Reg async_conn_funcs[] = {
	{"set_connect_cb", async_conn_set_connect_cb},
	{"set_disconnect_cb", async_conn_set_disconnect_cb},
	{"is_connected", async_conn_is_connected},
	{"connect", async_conn_connect},
	{"exec", async_conn_execute},
	{"execs", async_conn_execute_str},
	{"disconnect", async_conn_disconnect},
	{NULL, NULL},
};

static int async_conn_gc(lua_State *L) 
{
	redisAsyncContext *c = check_redis_conn(L, 1);
	if (c == NULL) {
		LAHIREDIS_DLOG("c=%p release already", c);
		return 0;
	}
	LAHIREDIS_DLOG("async_conn_gc is called,state=%p", L);
	if (LAHIREDIS_IS_CONN_CONNECTED(c)) {
		redisAsyncDisconnect(c);
	}
	/* TODO a bug here */
	return 0;
}

static int open_async_conn(lua_State *L)
{
	luaL_newmetatable(L, LAHIREDIS_CONN_CLASS);
	lua_newtable(L);				/*L: ..., {}, {}*/
	luaL_register(L, NULL, async_conn_funcs);		/*L: ..., {}, funcs*/
	lua_setfield(L, -2, "__index");			/*L: ..., {__index = funcs_tbl}*/

	lua_pushcfunction(L, async_conn_gc);		/*L: ..., {__index = funcs_tbl}, gc_func*/
	lua_setfield(L, -2, "__gc");			/*L: ..., {__index = funcs_tbl, __gc = gc_func}*/
	return 1;
}

static int new_async_conn(lua_State *L) 
{
        const char *host = luaL_checkstring(L, 1);
        short port = luaL_checkinteger(L, 2);
        redisAsyncContext *c = redisAsyncConnect(host, port);
	LAHIREDIS_DLOG("new connect,c=%p", c);
	if (c->err) {
		lua_pushboolean(L, 0);
		lua_pushstring(L, c->errstr);
		LAHIREDIS_DLOG("lua_state on new failed, l=%p,c=%p", L, c);
		/* redisAsyncFree(c);	leaked */
		return 2;
	}
	lahiredis_aysnc_conn_t *conn = (lahiredis_aysnc_conn_t *)calloc(1, sizeof(lahiredis_aysnc_conn_t));
	conn->L = L;
	lua_pushthread(L);
	conn->L_ref = lua_ref(L, 1);
	if (c->data) {
		free(c->data);
	}
	c->data = conn;
	lua_settop(L, 0);

	redisAsyncContext **ud = (redisAsyncContext **)lua_newuserdata(L, sizeof(void *));
	*ud = c;
	LAHIREDIS_DLOG("lua_state on new l=%p,c=%p,conn=%p,ud=%p", L, c, conn, ud);
	luaL_getmetatable(L, LAHIREDIS_CONN_CLASS);
	lua_setmetatable(L, -2);
	return 1;
}


static luaL_Reg lahiredis_async_funcs[] = {
	{"new", new_async_conn},
	{NULL, NULL},
};

static void set_info (lua_State *L) {
	LAHIREDIS_SET_CONST_STR(L, _DESCRIPTION, LIBNAME_LAHIREDIS " is a Lua library developed to complement the set of functions related to async hiredis.");
	LAHIREDIS_SET_CONST_STR(L, _AUTHOR, "Zhu Peng(rocaltair@gmail.com)");

        lua_pushliteral(L, "_VERSION");
        lua_pushnumber(L, LIB_HIREDIS_VERSION);
        lua_settable(L, -3);
	
	/* {{ backend */
	LAHIREDIS_SET_CONST_INTEGER(L, LAHIREDIS_BACKEND_LIBEV, LAHIREDIS_BACKEND_LIBEV);
	LAHIREDIS_SET_CONST_INTEGER(L, LAHIREDIS_BACKEND_LIBEVENT, LAHIREDIS_BACKEND_LIBEVENT);
	/* }} backend */

	/* {{ execution status*/
	LAHIREDIS_SET_CONST_INTEGER(L, REDIS_BLOCK, REDIS_BLOCK);
	LAHIREDIS_SET_CONST_INTEGER(L, REDIS_CONNECTED, REDIS_CONNECTED);
	LAHIREDIS_SET_CONST_INTEGER(L, REDIS_DISCONNECTING, REDIS_DISCONNECTING);
	LAHIREDIS_SET_CONST_INTEGER(L, REDIS_FREEING, REDIS_FREEING);
	LAHIREDIS_SET_CONST_INTEGER(L, REDIS_IN_CALLBACK, REDIS_IN_CALLBACK);
	LAHIREDIS_SET_CONST_INTEGER(L, REDIS_SUBSCRIBED, REDIS_SUBSCRIBED);
	LAHIREDIS_SET_CONST_INTEGER(L, REDIS_MONITORING, REDIS_MONITORING);
	/* }} execution status*/

	/* {{callback status */
	LAHIREDIS_SET_CONST_INTEGER(L, REDIS_ERR, REDIS_ERR);
	LAHIREDIS_SET_CONST_INTEGER(L, REDIS_OK, REDIS_OK);
	/*}} callback status*/

	/*{{ Reply*/
	LAHIREDIS_SET_CONST_INTEGER(L, REDIS_REPLY_STRING, REDIS_REPLY_STRING);
	LAHIREDIS_SET_CONST_INTEGER(L, REDIS_REPLY_ARRAY, REDIS_REPLY_ARRAY);
	LAHIREDIS_SET_CONST_INTEGER(L, REDIS_REPLY_INTEGER, REDIS_REPLY_INTEGER);
	LAHIREDIS_SET_CONST_INTEGER(L, REDIS_REPLY_NIL, REDIS_REPLY_NIL);
	LAHIREDIS_SET_CONST_INTEGER(L, REDIS_REPLY_STATUS, REDIS_REPLY_STATUS);
	LAHIREDIS_SET_CONST_INTEGER(L, REDIS_REPLY_ERROR, REDIS_REPLY_ERROR);
	/*}} Reply*/
}

int luaopen_lahiredis (lua_State *L) {
#if LUA_VERSION_NUM < 502
# pragma message "set main thread LUA_RIDX_MAINTHREAD"
	/* you should call this in main thread*/
	assert(lua_pushthread(L));
	lua_rawseti(L,  LUA_REGISTRYINDEX, LUA_RIDX_MAINTHREAD);
	lua_settop(L, 0);
#endif
	open_async_conn(L);
	luaL_newlib (L, lahiredis_async_funcs);
	lua_pushvalue(L, -1);
	lua_setglobal(L, LIBNAME_LAHIREDIS);
	set_info (L);
	return 1;
}

