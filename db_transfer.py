#!/usr/bin/python
# -*- coding: UTF-8 -*-

import logging
import cymysql
import time
import sys
from server_pool import ServerPool
import Config
import traceback
from shadowsocks import common

class DbTransfer(object):

	instance = None

	def __init__(self):
		import threading
		self.last_get_transfer = {}
		self.event = threading.Event()

	@staticmethod
	def get_instance():
		if DbTransfer.instance is None:
			DbTransfer.instance = DbTransfer()
		return DbTransfer.instance

	def push_db_all_user(self):
# 		logging.warning('Watch out!')
		conn = cymysql.connect(host=Config.MYSQL_HOST, port=Config.MYSQL_PORT, user=Config.MYSQL_USER,passwd=Config.MYSQL_PASS, db=Config.MYSQL_DB, charset='utf8')
		cur = conn.cursor()
		nodequery = 'SELECT id, traffic_rate FROM `ss_node`'
		nodenamequery = """ WHERE name = '""" + Config.MANAGE_NODE_NAME+"""'"""
		nodesql = nodequery + nodenamequery
# 		logging.warning(nodesql)
		cur.execute(nodesql)
		for r in cur.fetchall():
			Config.MYSQL_TRANSFER_MUL = r[1]
			Config.MANAGE_NODE_ID = r[0]
# 			logging.warning('r[0]%s' % r[0])
			
		cur.close()
		conn.close()
# 		logging.warning('Watch out End! %s' % Config.MYSQL_TRANSFER_MUL)
		#更新用户流量到数据库
		last_transfer = self.last_get_transfer
		curr_transfer = ServerPool.get_instance().get_servers_transfer()
		#上次和本次的增量
		dt_transfer = {}
		for id in curr_transfer.keys():
			if id in last_transfer:
				if last_transfer[id][0] == curr_transfer[id][0] and last_transfer[id][1] == curr_transfer[id][1]:
					continue
				elif curr_transfer[id][0] == 0 and curr_transfer[id][1] == 0:
					continue
				elif last_transfer[id][0] <= curr_transfer[id][0] and \
				last_transfer[id][1] <= curr_transfer[id][1]:
					dt_transfer[id] = [int((curr_transfer[id][0] - last_transfer[id][0]) * Config.MYSQL_TRANSFER_MUL),
										int((curr_transfer[id][1] - last_transfer[id][1]) * Config.MYSQL_TRANSFER_MUL)]
				else:
					dt_transfer[id] = [int(curr_transfer[id][0] * Config.MYSQL_TRANSFER_MUL),
										int(curr_transfer[id][1] * Config.MYSQL_TRANSFER_MUL)]
			else:
				if curr_transfer[id][0] == 0 and curr_transfer[id][1] == 0:
					continue
				dt_transfer[id] = [int(curr_transfer[id][0] * Config.MYSQL_TRANSFER_MUL),
									int(curr_transfer[id][1] * Config.MYSQL_TRANSFER_MUL)]

		self.last_get_transfer = curr_transfer
		query_head = 'UPDATE user'
		query_sub_when = ''
		query_sub_when2 = ''
		query_sub_in = None
		last_time = time.time()
		for id in dt_transfer.keys():
			if dt_transfer[id][0] == 0 and dt_transfer[id][1] == 0:
				continue
			query_sub_when += ' WHEN %s THEN u+%s' % (id, dt_transfer[id][0])
			query_sub_when2 += ' WHEN %s THEN d+%s' % (id, dt_transfer[id][1])
			if query_sub_in is not None:
				query_sub_in += ',%s' % id
			else:
				query_sub_in = '%s' % id
		if query_sub_when == '':
			return
		query_sql = query_head + ' SET u = CASE port' + query_sub_when + \
					' END, d = CASE port' + query_sub_when2 + \
					' END, t = ' + str(int(last_time)) + \
					' WHERE port IN (%s)' % query_sub_in
# 		logging.warning("time.time() %s" % time.time())
# 		logging.warning("id %s" % id)
# 		logging.warning("dt_transfer[id][0] %s" % dt_transfer[id][0])
# 		logging.warning("dt_transfer[id][1] %s" % dt_transfer[id][1])
# 		logging.warning("query_sql:"+query_sql)
		record_sql_head = 'INSERT INTO `admin_ss`.`user_traffic_log` (`user_id`, `u`, `d`, `node_id`, `rate`, `traffic`, `log_time`) VALUES ('
		record_sql_userid = '%s, ' % id
		record_sql_useru = '%s, ' % (dt_transfer[id][0]/Config.MYSQL_TRANSFER_MUL)
		record_sql_userd = '%s, ' % (dt_transfer[id][1]/Config.MYSQL_TRANSFER_MUL)
		record_sql_usernode = '%s, ' % int(Config.MANAGE_NODE_ID)
		record_sql_userrate = '%f, ' % Config.MYSQL_TRANSFER_MUL
		record_sql_usertxrx = '%s+%s,' % (dt_transfer[id][0],dt_transfer[id][1])
		record_sql_userlogtime = '%s' % int(time.time())
		record_sql_tail = ')'
		record_sql = record_sql_head+record_sql_userid+record_sql_useru+record_sql_userd+record_sql_usernode+record_sql_userrate+record_sql_usertxrx+record_sql_userlogtime+record_sql_tail
# 		logging.warning("record_sql:"+record_sql)
		conn = cymysql.connect(host=Config.MYSQL_HOST, port=Config.MYSQL_PORT, user=Config.MYSQL_USER,
								passwd=Config.MYSQL_PASS, db=Config.MYSQL_DB, charset='utf8')
		cur = conn.cursor()
		cur.execute(query_sql)
		cur.execute(record_sql)
		cur.close()
		conn.commit()
		conn.close()

	@staticmethod
	def pull_db_all_user():
		#数据库所有用户信息
		try:
			import switchrule
			reload(switchrule)
			keys = switchrule.getKeys()
		except Exception as e:
			keys = ['port', 'u', 'd', 'transfer_enable', 'passwd', 'enable' ]
		reload(cymysql)
		conn = cymysql.connect(host=Config.MYSQL_HOST, port=Config.MYSQL_PORT, user=Config.MYSQL_USER,
								passwd=Config.MYSQL_PASS, db=Config.MYSQL_DB, charset='utf8')
		cur = conn.cursor()
		cur.execute("SELECT " + ','.join(keys) + " FROM user")
		rows = []
		for r in cur.fetchall():
			d = {}
			for column in xrange(len(keys)):
				d[keys[column]] = r[column]
			rows.append(d)
		cur.close()
		conn.close()
		return rows

	@staticmethod
	def del_server_out_of_bound_safe(last_rows, rows):
		#停止超流量的服务
		#启动没超流量的服务
		#需要动态载入switchrule，以便实时修改规则
		try:
			import switchrule
			reload(switchrule)
		except Exception as e:
			logging.error('load switchrule.py fail')
		cur_servers = {}
		for row in rows:
			try:
				allow = switchrule.isTurnOn(row) and row['enable'] == 1 and row['u'] + row['d'] < row['transfer_enable']
			except Exception as e:
				allow = False

			port = row['port']
			passwd = common.to_bytes(row['passwd'])

			if port not in cur_servers:
				cur_servers[port] = passwd
			else:
				logging.error('more than one user use the same port [%s]' % (port,))
				continue

			if ServerPool.get_instance().server_is_run(port) > 0:
				if not allow:
					logging.info('db stop server at port [%s]' % (port,))
					ServerPool.get_instance().cb_del_server(port)
				elif (port in ServerPool.get_instance().tcp_servers_pool and ServerPool.get_instance().tcp_servers_pool[port]._config['password'] != passwd) \
					or (port in ServerPool.get_instance().tcp_ipv6_servers_pool and ServerPool.get_instance().tcp_ipv6_servers_pool[port]._config['password'] != passwd):
					#password changed
					logging.info('db stop server at port [%s] reason: password changed' % (port,))
					ServerPool.get_instance().cb_del_server(port)
					ServerPool.get_instance().new_server(port, passwd)

			elif allow and ServerPool.get_instance().server_run_status(port) is False:
				logging.info('db start server at port [%s] pass [%s]' % (port, passwd))
				ServerPool.get_instance().new_server(port, passwd)

		for row in last_rows:
			if row['port'] in cur_servers:
				pass
			else:
				logging.info('db stop server at port [%s] reason: port not exist' % (row['port']))
				ServerPool.get_instance().cb_del_server(row['port'])

	@staticmethod
	def del_servers():
		for port in ServerPool.get_instance().tcp_servers_pool.keys():
			if ServerPool.get_instance().server_is_run(port) > 0:
					ServerPool.get_instance().cb_del_server(port)
		for port in ServerPool.get_instance().tcp_ipv6_servers_pool.keys():
			if ServerPool.get_instance().server_is_run(port) > 0:
					ServerPool.get_instance().cb_del_server(port)

	@staticmethod
	def thread_db():
		import socket
		import time
		timeout = 60
		socket.setdefaulttimeout(timeout)
		last_rows = []
		try:
			while True:
				try:
					DbTransfer.get_instance().push_db_all_user()
					rows = DbTransfer.get_instance().pull_db_all_user()
					DbTransfer.del_server_out_of_bound_safe(last_rows, rows)
					last_rows = rows
				except Exception as e:
					trace = traceback.format_exc()
					logging.error(trace)
					#logging.warn('db thread except:%s' % e)
				if DbTransfer.get_instance().event.wait(15):
					break
		except KeyboardInterrupt as e:
			pass
		DbTransfer.del_servers()
		ServerPool.get_instance().stop()

	@staticmethod
	def thread_db_stop():
		DbTransfer.get_instance().event.set()

