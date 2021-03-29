package tables;

import com.mysql.cj.jdbc.MysqlDataSource;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.resources.SpringResource;
import org.jetbrains.annotations.Nullable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.cache.Cache;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;
/*
https://github.com/zhimx1988/ignite-demo-driverDML/blob/3f1e2e082b1758aa512654bdb98a8e5fc268f7fe/src/main/java/ignite/demo/PersonStore.java
 */

public class PersonStore implements CacheStore<Integer, Person> {

    public final MysqlDataSource dataSource = getDateSource();

    static public MysqlDataSource getDateSource() {
        MysqlDataSource dateSource = new MysqlDataSource();
        dateSource.setURL("jdbc:mysql://localhost:3306/ZRDB");
        dateSource.setUser("ranger");
        dateSource.setPassword("123456");
        //dateSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
        return dateSource;
    }

    // This method is called whenever IgniteCache.loadCache() method is called.
    @Override
    public void loadCache(IgniteBiInClosure<Integer, Person> igniteBiInClosure, @Nullable Object... objects) throws CacheLoaderException {
        System.out.println(">> Loading cache from store...");
        try (Connection conn = dataSource.getConnection()) {
            try (PreparedStatement st = conn.prepareStatement("select * from Person")) {
                try (ResultSet rs = st.executeQuery()) {
                    while (rs.next()) {
                        Person person = new Person(rs.getInt(1), rs.getInt(2), rs.getString(3), rs.getInt(4));
                        igniteBiInClosure.apply(person.getId(), person);
                    }
                }
            }
        } catch (SQLException e) {
            throw new CacheLoaderException("Failed to load values from cache store.", e);
        }
    }

    // This method is called whenever IgniteCache.get() method is called.
    @Override
    public Person load(Integer key) throws CacheLoaderException {
        System.out.println(">> Loading person from store...");
        try (Connection conn = dataSource.getConnection()) {
            try (PreparedStatement st = conn.prepareStatement("select * from Person where id = ?")) {
                st.setString(1, key.toString());
                ResultSet rs = st.executeQuery();
                return rs.next() ? new Person(rs.getInt(1), rs.getInt(2), rs.getString(3), rs.getInt(4)) : null;
            }
        } catch (SQLException e) {
            throw new CacheLoaderException("Failed to load values from cache store.", e);
        }
    }

    // IgniteCache.put()
    @Override
    public void write(Cache.Entry<? extends Integer, ? extends Person> entry) throws CacheWriterException {
        System.out.println(">> Inserting person from store...");
        Person person = entry.getValue();
        Connection conn = null;
        try {
            conn = dataSource.getConnection();
            PreparedStatement st = conn.prepareStatement("insert into Person(id,orgId,name,salary) values(?,?,?,?)");
            st.setInt(1, person.getId());
            st.setInt(2, person.getOrgId());
            st.setString(3, person.getName());
            st.setInt(4, person.getSalary());
            st.execute();
            //st.executeUpdate();
            System.out.println("Insert " + person.getId() + ", " + person.getOrgId() + ", " + person.getName() + ", " + person.getSalary());
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    // IgniteCache.remove()
    @Override
    public void delete(Object o) throws CacheWriterException {

    }

    @Override
    public Map<Integer, Person> loadAll(Iterable<? extends Integer> iterable) throws CacheLoaderException {
        return null;
    }

    @Override
    public void deleteAll(Collection<?> collection) throws CacheWriterException {

    }

    @Override
    public void writeAll(Collection<Cache.Entry<? extends Integer, ? extends Person>> collection) throws CacheWriterException {

    }

    @Override
    public void sessionEnd(boolean b) throws CacheWriterException {

    }
}
