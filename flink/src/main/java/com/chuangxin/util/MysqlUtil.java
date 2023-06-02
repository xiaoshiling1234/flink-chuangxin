package com.chuangxin.util;

import com.chuangxin.common.GlobalConfig;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MysqlUtil {
    
    public static void insert(String tableName, Map<String, Object> data) {
        Connection conn = null;
        PreparedStatement ps = null;
        try {
            conn = getConnection();
            StringBuilder sql = new StringBuilder("INSERT INTO ").append(tableName).append(" (");
            StringBuilder values = new StringBuilder(" VALUES (");
            List<Object> params = new ArrayList<>();
            for (Map.Entry<String, Object> entry : data.entrySet()) {
                sql.append(entry.getKey()).append(",");
                values.append("?,");
                params.add(entry.getValue());
            }
            sql.deleteCharAt(sql.length() - 1).append(")");
            values.deleteCharAt(values.length() - 1).append(")");
            sql.append(values);
            ps = conn.prepareStatement(sql.toString());
            for (int i = 0; i < params.size(); i++) {
                ps.setObject(i + 1, params.get(i));
            }
            ps.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            close(conn, ps, null);
        }
    }

    public static void update(String tableName, Map<String, Object> data, String whereClause, Object... whereArgs) {
        Connection conn = null;
        PreparedStatement ps = null;
        try {
            conn = getConnection();
            StringBuilder sql = new StringBuilder("UPDATE ").append(tableName).append(" SET ");
            List<Object> params = new ArrayList<>();
            for (Map.Entry<String, Object> entry : data.entrySet()) {
                sql.append(entry.getKey()).append("=?,");
                params.add(entry.getValue());
            }
            sql.deleteCharAt(sql.length() - 1);
            if (whereClause != null) {
                sql.append(" WHERE ").append(whereClause);
                for (Object arg : whereArgs) {
                    params.add(arg);
                }
            }
            ps = conn.prepareStatement(sql.toString());
            for (int i = 0; i < params.size(); i++) {
                ps.setObject(i + 1, params.get(i));
            }
            ps.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            close(conn, ps, null);
        }
    }

    public static void delete(String tableName, String whereClause, Object... whereArgs) {
        Connection conn = null;
        PreparedStatement ps = null;
        try {
            conn = getConnection();
            StringBuilder sql = new StringBuilder("DELETE FROM ").append(tableName);
            if (whereClause != null) {
                sql.append(" WHERE ").append(whereClause);
            }
            ps = conn.prepareStatement(sql.toString());
            for (int i = 0; i < whereArgs.length; i++) {
                ps.setObject(i + 1, whereArgs[i]);
            }
            ps.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            close(conn, ps, null);
        }
    }

    public static List<Map<String, Object>> query(String sql, Object... args) {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        List<Map<String, Object>> result = new ArrayList<>();
        try {
            conn = getConnection();
            ps = conn.prepareStatement(sql);
            for (int i = 0; i < args.length; i++) {
                ps.setObject(i + 1, args[i]);
            }
            rs = ps.executeQuery();
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            while (rs.next()) {
                Map<String, Object> row = new HashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    row.put(metaData.getColumnName(i), rs.getObject(i));
                }
                result.add(row);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            close(conn, ps, rs);
        }
        return result;
    }

    private static Connection getConnection() throws SQLException {
        String url = GlobalConfig.MYSQL_URL;
        String user = GlobalConfig.MYSQL_USER;
        String password = GlobalConfig.MYSQL_PASSWORD;
        return DriverManager.getConnection(url, user, password);
    }

    private static void close(Connection conn, PreparedStatement ps, ResultSet rs) {
        try {
            if (rs != null) {
                rs.close();
            }
            if (ps != null) {
                ps.close();
            }
            if (conn != null) {
                conn.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    
}
