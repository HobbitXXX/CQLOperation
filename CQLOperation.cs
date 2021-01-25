using System;
using System.Data;
using System.Data.SqlClient;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Neo4j.Driver;
using System.Web.Script.Serialization;

namespace ZYAPP.Models
{
    /**********************************************************************************************
    *  @author   zxx
    *  @Build at:2020/12/3 
    *  摘要：
    *       所有模型类对数据库的操作包含数据库的链接,关闭，以及增,删,改,查
    *  修改历史：   
    *          日期           版本               修改人             修改内容
    *        2020-12-03       1.0                zxx             创建初始版本
    **********************************************************************************************/
    public class CQLOperation
    {
        private string _dbHost = "bolt://连接地址:7687";
        private string _dbUser = "用户名";
        private string _dbPassword = "用户密码";
        private IDriver _driver;

        /**********************************************************************************************
         *  @author   zxx
         *  @Build at:2020/12/11  
         *  摘要：
         *       执行cql语句
         *  参数：
         *       Cql:字符串  需要执行的cql语句
         *       return_label:整型  根据语句是否包含return判断查询结果是否需要返回
         *  返回：
         *      字符串json数据
         *  修改历史：   
         *          日期           版本               修改人             修改内容
         *        2020-12-11        1.0                zxx             创建初始版本     
         **********************************************************************************************/
        public string ExcuteQuery(string Cql,int return_label)
        {
            var data = "";
            try
            {
                _driver = GraphDatabase.Driver(_dbHost, AuthTokens.Basic(_dbUser, _dbPassword));
                using (var session = _driver.Session())
                {
                    if (return_label == 1)
                    {
                        var greeting = session.WriteTransaction(tx =>
                        {
                            var result = tx.Run(Cql);
                            return result.ToList();
                        });
                        data = ToJson(greeting);
                    }
                    else {
                        session.Run(Cql);
                        List<string> res =new List<string>() { };
                        data = ToJson(res);
                    }
                    return data;
                }
            }
            catch (SqlException ex)
            {
                throw new Exception(ex.ToString());
            }
            finally
            {
            }
        }

        /**********************************************************************************************
         *  @author   zxx
         *  @Build at:2020/12/11 
         *  摘要：
         *       这是数据库操作函数的统一入口函数，根据输入参数返回执行结果
         *  参数：
         *      "command":要执行的操作类型：字符串数组        eg:create/merge/match/delete/remove/set/return 
         *      "label_name":create或merge语句中所需的节点或关系标签名：二维字符串列表        根据操作类型不同可为null 
         *      "json_data":属性json数据：二维字符串列表        根据操作类型不同可为null
         *      "match_label_name":查询时的节点或关系标签名：二维字符串列表        根据操作类型不同可为null 
         *      "match_json_data":查询时的属性json数据：二维字符串列表        根据操作类型不同可为null
         *      "cols":delete或remove的节点（关系）或属性字段列表：字符串列表        根据操作类型不同可为null
         *      "values":修改属性时cols列表对应的值：字符串列表        根据操作类型不同可为null
         *      "return_cols":查询时需要返回的节点（关系）或属性字段列表：字符串列表        根据操作类型不同可为null 
         *      "conditions":match查询时附加的条件字符串        根据操作类型不同可为null  
         *  返回：
         *      字符串json数据
         *  修改历史：   
         *          日期           版本               修改人             修改内容
         *        2020-12-11       1.0                zxx             创建初始版本
         **********************************************************************************************/
        public string ProcessData(string[] command, List<List<string>> label_name = null, List<List<string>> json_data = null, List<List<string>> match_label_name = null, List<List<string>> match_json_data = null, List<string> cols = null, List<string> values = null, List<string> return_cols = null, string conditions = null)
        {
            string _sqlsentence = "";
            var return_label = 0;
            for (var i=0; i<command.Length; i++) {
                if (string.Equals(command[i], "create", StringComparison.OrdinalIgnoreCase))
                {
                    _sqlsentence += BuildCreateDataStr(label_name, json_data);
                }
                else if (string.Equals(command[i], "merge", StringComparison.OrdinalIgnoreCase))
                {
                    _sqlsentence += BuildMergeDataStr(label_name, json_data);
                }
                else if (string.Equals(command[i], "match", StringComparison.OrdinalIgnoreCase))
                {
                    _sqlsentence += BuildMatchDataStr(match_label_name, match_json_data, conditions);
                }
                else if (string.Equals(command[i], "delete", StringComparison.OrdinalIgnoreCase))
                {
                    _sqlsentence += BuildDeleteDataStr(cols);
                }
                else if (string.Equals(command[i], "remove", StringComparison.OrdinalIgnoreCase))
                {
                    _sqlsentence += BuildRemoveDataStr(cols);
                }
                else if (string.Equals(command[i], "set", StringComparison.OrdinalIgnoreCase))
                {
                    _sqlsentence += BuildSetDataStr(cols, values);
                }
                else if (string.Equals(command[i], "return", StringComparison.OrdinalIgnoreCase))
                {
                    _sqlsentence += BuildReturnDataStr(return_cols);
                    return_label = 1;
                }
                else
                {
                    throw new Exception("Cql语句操作关键字出现错误");
                }
            }
            return ExcuteQuery(_sqlsentence, return_label);
        }

        /**********************************************************************************************
        *  @author   zxx
        *  @Build at:2020/12/11 
        *  摘要：
        *       生成create执行语句的方法函数
        *       json数据格式（创建节点）如：[["{name:'需求'}"],["{content:'需求内容'}"]]
        *       json数据格式（创建关系）如：[["{name:'需求'}", "{content:'需求内容'}", "{RELATIONSHIP:'requirement'}"]]
        *  参数：
        *      "label_name":节点或关系标签名：二维字符串列表        根据操作类型不同可为null 
        *      "json_data":属性json数据：二维字符串列表        根据操作类型不同可为null
        *  返回：
        *      生成的Cql可执行语句：字符串
        *  修改历史：   
        *          日期           版本               修改人             修改内容
        *        2020-12-11       1.0                zxx             创建初始版本
        **********************************************************************************************/
        public string BuildCreateDataStr(List<List<string>> label_name, List<List<string>> json_data)
        {
            string _sqlsentence = " create ";
            for (var i = 0; i < label_name.Count; i++)
            {
                if (label_name[i].Count == 1)
                {
                    _sqlsentence += " (" + label_name[i][0];
                    if (json_data != null)
                    {
                        _sqlsentence += json_data[i][0];
                    }
                    _sqlsentence += ")";
                }
                else if (label_name[i].Count == 3)
                {
                    if (json_data != null)
                    {
                        _sqlsentence += " (" + label_name[i][0] + json_data[i][0] + ") -[";
                        _sqlsentence += label_name[i][2] + json_data[i][2] + "]->";
                        _sqlsentence += " (" + label_name[i][1] + json_data[i][1] + ")";
                    }
                    else
                    {
                        _sqlsentence += " (" + label_name[i][0] + ") -[";
                        _sqlsentence += label_name[i][2] + "]->";
                        _sqlsentence += " (" + label_name[i][1] + ")";
                    }
                }
                if (i < label_name.Count - 1)
                {
                    _sqlsentence += ",";
                }
            }
            return _sqlsentence;
        }

        /**********************************************************************************************
        *  @author   zxx
        *  @Build at:2020/12/11 
        *  摘要：
        *       生成merge执行语句的方法函数
        *  参数：
        *      "label_name":节点或关系标签名：二维字符串列表        根据操作类型不同可为null 
        *      "json_data":属性json数据：二维字符串列表        根据操作类型不同可为null 
        *  返回：
        *      生成的Cql可执行语句：字符串
        *  修改历史：   
        *          日期           版本               修改人             修改内容
        *        2020-12-11       1.0                zxx             创建初始版本
        **********************************************************************************************/
        public string BuildMergeDataStr(List<List<string>> label_name, List<List<string>> json_data)
        {
            string _sqlsentence = " merge ";
            for (var i = 0; i < label_name.Count; i++)
            {
                if (label_name[i].Count == 1)
                {
                    _sqlsentence += " (" + label_name[i][0];
                    if (json_data != null)
                    {
                        _sqlsentence += json_data[i][0];
                    }
                    _sqlsentence += ")";
                }
                else if (label_name[i].Count == 3)
                {
                    if (json_data != null)
                    {
                        _sqlsentence += " (" + label_name[i][0] + json_data[i][0] + ") -[";
                        _sqlsentence += label_name[i][2] + json_data[i][2] + "]-";
                        _sqlsentence += " (" + label_name[i][1] + json_data[i][1] + ")";
                    }
                    else
                    {
                        _sqlsentence += " (" + label_name[i][0] + ") -[";
                        _sqlsentence += label_name[i][2] + "]-";
                        _sqlsentence += " (" + label_name[i][1] + ")";
                    }
                }
                if (i < label_name.Count - 1)
                {
                    _sqlsentence += ",";
                }
            }
            return _sqlsentence;
        }

        /**********************************************************************************************
        *  @author   zxx
        *  @Build at:2020/12/11 
        *  摘要：
        *       生成match执行语句的方法函数
        *  参数：
        *      "match_label_name":节点或关系标签名：二维字符串列表        根据操作类型不同可为null 
        *      "match_json_data":属性json数据：二维字符串列表        根据操作类型不同可为null
        *      "conditions":match查询时附加的条件字符串        根据操作类型不同可为null 
        *  返回：
        *      生成的Cql可执行语句：字符串
        *  修改历史：   
        *          日期           版本               修改人             修改内容
        *        2020-12-11       1.0                zxx             创建初始版本
        **********************************************************************************************/
        public string BuildMatchDataStr(List<List<string>> match_label_name, List<List<string>> match_json_data = null, string conditions = null)
        {
            string _sqlsentence = " match ";
            for (var i = 0; i < match_label_name.Count; i++)
            {
                for (var j=0; j< match_label_name[i].Count; j++) {
                    if (j > 2 && j % 2 == 1)
                    {
                        if (match_json_data != null && match_json_data[i] != null)
                        {
                            _sqlsentence += "-[ " + match_label_name[i][j] + match_json_data[i][j] + " ]-";
                        }
                        else
                        {
                            _sqlsentence += "-[ " + match_label_name[i][j] + " ]-";
                        }
                    }
                    else if (j == 2)
                    {
                        if (match_json_data != null && match_json_data[1] != null)
                        {
                            _sqlsentence += "-[ " + match_label_name[i][2] + match_json_data[i][2] + " ]-";
                            _sqlsentence += " (" + match_label_name[i][1] + match_json_data[i][1] + ") ";
                        }
                        else
                        {
                            _sqlsentence += "-[ " + match_label_name[i][2] + " ]-";
                            _sqlsentence += " (" + match_label_name[i][1] + ") ";
                        }
                    }
                    else if (j != 1 )
                    {
                        if (match_json_data != null && match_json_data[i] != null)
                        {
                            _sqlsentence += " (" + match_label_name[i][j] + match_json_data[i][j] + ") ";
                        }
                        else
                        {
                            _sqlsentence += " (" + match_label_name[i][j] + ") ";
                        }
                    }
                    
                }
                if (i < match_label_name.Count - 1)
                {
                    _sqlsentence += ",";
                }
            }
            if (conditions != null) {
                _sqlsentence += conditions;
            }
            return _sqlsentence;
        }

        /**********************************************************************************************
        *  @author   zxx
        *  @Build at:2020/12/11 
        *  摘要：
        *       生成delete执行语句的方法函数
        *  参数：
        *      "cols":节点（关系）或属性字段列表：字符串列表        根据操作类型不同可为null
        *  返回：
        *      生成的Cql可执行语句：字符串
        *  修改历史：   
        *          日期           版本               修改人             修改内容
        *        2020-12-11       1.0                zxx             创建初始版本
        **********************************************************************************************/
        public string BuildDeleteDataStr(List<string> cols)
        {
            string _sqlsentence = " delete ";
            for (var i = 0; i < cols.Count; i++)
            {
                _sqlsentence += cols[i];
                if (i < cols.Count - 1)
                {
                    _sqlsentence += ",";
                }
            }
            return _sqlsentence;
        }

        /**********************************************************************************************
        *  @author   zxx
        *  @Build at:2020/12/11 
        *  摘要：
        *       生成remove执行语句的方法函数
        *  参数：
        *      "cols":节点（关系）或属性字段列表：字符串列表        根据操作类型不同可为null
        *  返回：
        *      生成的Cql可执行语句：字符串
        *  修改历史：   
        *          日期           版本               修改人             修改内容
        *        2020-12-11       1.0                zxx             创建初始版本
        **********************************************************************************************/
        public string BuildRemoveDataStr(List<string> cols)
        {
            string _sqlsentence = " remove ";
            for (var i = 0; i < cols.Count; i++)
            {
                _sqlsentence += cols[i];
                if (i < cols.Count - 1)
                {
                    _sqlsentence += ",";
                }
            }
            return _sqlsentence;
        }

        /**********************************************************************************************
        *  @author   zxx
        *  @Build at:2020/12/11 
        *  摘要：
        *       生成set执行语句的方法函数
        *  参数：
        *      "cols":节点（关系）或属性字段列表：字符串列表        根据操作类型不同可为null
        *      "values":修改属性时cols列表对应的值：字符串列表        根据操作类型不同可为null
        *  返回：
        *      生成的Cql可执行语句：字符串
        *  修改历史：   
        *          日期           版本               修改人             修改内容
        *        2020-12-11       1.0                zxx             创建初始版本
        **********************************************************************************************/
        public string BuildSetDataStr(List<string> cols, List<string> values)
        {
            string _sqlsentence = " set ";
            for (var i = 0; i < cols.Count; i++)
            {
                _sqlsentence += cols[i] + "=" + values[i];
                if (i < cols.Count - 1)
                {
                    _sqlsentence += ",";
                }
            }
            return _sqlsentence;
        }

        /**********************************************************************************************
        *  @author   zxx
        *  @Build at:2020/12/11 
        *  摘要：
        *       生成return执行语句的方法函数
        *  参数：
        *      "return_cols":需要返回的节点（关系）或属性字段列表：字符串列表        根据操作类型不同可为null 
        *  返回：
        *      生成的Cql可执行语句：字符串
        *  修改历史：   
        *          日期           版本               修改人             修改内容
        *        2020-12-11       1.0                zxx             创建初始版本
        **********************************************************************************************/
        public string BuildReturnDataStr(List<string> return_cols)
        {
            string _sqlsentence = " return ";
            for (var i = 0; i < return_cols.Count; i++)
            {
                _sqlsentence += return_cols[i];
                if (i < return_cols.Count - 1)
                {
                    _sqlsentence += ",";
                }
            }
            return _sqlsentence;
        }

        //生成Json
        protected string ToJson(object obj)
        {
            string jsonData = (new JavaScriptSerializer()).Serialize(obj);
            return jsonData;
        }
    }
}
