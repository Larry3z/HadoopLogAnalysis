package com.demo.model;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * Created by guang_chen on 2016/12/1.
 */
public class IP implements Writable, DBWritable {
    private java.util.Date time;
    private String ip;
    private int num;
    private java.util.Date createAt;

    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public IP(java.util.Date time, String ip, int num, java.util.Date createAt) {
        this.time = time;
        this.ip = ip;
        this.num = num;
        this.createAt = createAt;
    }

    public IP() {

    }

    @Override
    public void write(DataOutput out) throws IOException {
        String time_str = sdf.format(this.time);
        String createAt_str = sdf.format(this.createAt);
        out.writeUTF(time_str);
        out.writeUTF(this.ip);
        out.writeInt(this.num);
        out.writeUTF(createAt_str);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        try {
            String time_str = in.readUTF();
            this.time = sdf.parse(time_str);
            this.ip = in.readUTF();
            this.num = in.readInt();
            String createAt_str = in.readUTF();
            this.createAt = sdf.parse(createAt_str);
        } catch (ParseException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void write(PreparedStatement statement) throws SQLException {
        java.sql.Date timeSqlDate = new java.sql.Date(this.time.getTime());
        java.sql.Date createAtSqlDate = new java.sql.Date(this.createAt.getTime());
        statement.setDate(1, timeSqlDate);
        statement.setString(2, this.ip);
        statement.setInt(3, this.num);
        statement.setDate(4, createAtSqlDate);
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        this.time = resultSet.getDate(1);
        this.ip = resultSet.getString(2);
        this.num = resultSet.getInt(3);
        this.createAt = resultSet.getDate(4);
    }
}
