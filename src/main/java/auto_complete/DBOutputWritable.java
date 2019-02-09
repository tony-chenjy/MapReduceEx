package auto_complete;

import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author tony.chenjy
 * @date 2019/2/9 0009 12:11
 */
public class DBOutputWritable implements DBWritable{

    private String startingPhrase;
    private String followingWord;
    private Integer count;

    public DBOutputWritable(String startingPhrase, String followingWord, Integer count) {
        this.startingPhrase = startingPhrase;
        this.followingWord = followingWord;
        this.count = count;
    }

    @Override
    public void write(PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setString(1, this.startingPhrase);
        preparedStatement.setString(2, this.followingWord);
        preparedStatement.setInt(3, this.count);
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        // TODO
    }
}
