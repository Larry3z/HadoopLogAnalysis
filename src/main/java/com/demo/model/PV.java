package com.demo.model;

import org.apache.hadoop.io.Text;
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
public class PV implements Writable, DBWritable {
    private java.util.Date time;
    private String url;
    private int pv;
    private java.util.Date createAt;
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public PV(java.util.Date time, String url, int pv, java.util.Date createAt) {
        this.time = time;
        this.url = url;
        this.pv = pv;
        this.createAt = createAt;
    }

    public PV() {

    }

    @Override
    public void write(DataOutput out) throws IOException {
        String time_str = sdf.format(this.time);
        String createAt_str = sdf.format(this.createAt);
        out.writeUTF(time_str);
        Text.writeString(out, this.url);
        out.writeInt(this.pv);
        out.writeUTF(createAt_str);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        try {
            String time_str = in.readUTF();
            this.time = sdf.parse(time_str);
            this.url = Text.readString(in);
            this.pv = in.readInt();
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
        statement.setString(2, this.url);
        statement.setInt(3, this.pv);
        statement.setDate(4, createAtSqlDate);
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        this.time = resultSet.getDate(1);
        this.url = resultSet.getString(2);
        this.pv = resultSet.getInt(3);
        this.createAt = resultSet.getDate(4);
    }
}
