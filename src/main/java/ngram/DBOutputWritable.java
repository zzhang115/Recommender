import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by zzc on 7/24/17.
 */
public class DBOutputWritable implements DBWritable
{
    private String startingPhrase;
    private String followingPhrase;
    private int count;
    public DBOutputWritable(String startingPhrase, String followingPhrase, int count)
    {
        this.startingPhrase = startingPhrase;
        this.followingPhrase = followingPhrase;
        this.count = count;
    }

    public void readFields(ResultSet args) throws SQLException
    {
        this.startingPhrase = args.getString(1);
        this.followingPhrase = args.getString(2);
        this.count = args.getInt(3);
    }

    public void write(PreparedStatement stats) throws SQLException
    {
        stats.setString(1, startingPhrase);
        stats.setString(2, followingPhrase);
        stats.setInt(3, count);
    }
}
