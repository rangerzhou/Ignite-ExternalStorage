package initial;

import com.mysql.cj.jdbc.MysqlDataSource;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.store.jdbc.CacheJdbcPojoStoreFactory;
import org.apache.ignite.cache.store.jdbc.JdbcType;
import org.apache.ignite.cache.store.jdbc.JdbcTypeField;
import org.apache.ignite.cache.store.jdbc.dialect.MySQLDialect;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import tables.Person;
import tables.PersonStore;

import javax.cache.configuration.Factory;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.integration.CacheLoaderException;
import javax.sql.DataSource;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author ranger
 * https://www.gridgain.com/docs/latest/developers-guide/persistence/external-storage
 */
public class PersonStoreExample {
    public static void main(String[] args) {
        //System.setProperty("IGNITE_QUIET", "false");
        //Ignite ignite2 = Ignition.start("cluster-config.xml");
        IgniteConfiguration igniteCfg = new IgniteConfiguration();

        CacheConfiguration<Integer, Person> personCacheCfg = new CacheConfiguration<>();

        personCacheCfg.setName("PersonCache");
        personCacheCfg.setCacheMode(CacheMode.PARTITIONED);
        personCacheCfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);

        personCacheCfg.setReadThrough(true);
        personCacheCfg.setWriteThrough(true);
        // 设置后写是否启用的标志
        personCacheCfg.setWriteBehindEnabled(true);
        // 后写缓存的刷新频率，单位为毫秒
        personCacheCfg.setWriteBehindFlushFrequency(10);
        // 后写缓存的最大值，如果超过了这个限值，所有的缓存数据都会被刷入CacheStore然后写缓存被清空
        personCacheCfg.setWriteBehindFlushSize(128);
        // 后写缓存存储操作的操作数最大值,持久化后写每批次写入数量
        personCacheCfg.setWriteBehindBatchSize(10);
        // 执行缓存刷新的线程数
        personCacheCfg.setWriteBehindFlushThreadCount(8);


        CacheJdbcPojoStoreFactory<Integer, Person> factory = new CacheJdbcPojoStoreFactory<>();
        //factory.setDialect(new MySQLDialect());
        factory.setDataSourceFactory((Factory<DataSource>) () -> {
            MysqlDataSource mysqlDataSrc = new MysqlDataSource();
            mysqlDataSrc.setURL("jdbc:mysql://localhost:3306/ZRDB");
            mysqlDataSrc.setUser("ranger");
            mysqlDataSrc.setPassword("123456");
            return mysqlDataSrc;
        });

        PersonStore personStore = new PersonStore();
        Factory<? extends PersonStore> storeFactory = FactoryBuilder.factoryOf(personStore.getClass());

        JdbcType personType = new JdbcType();
        personType.setCacheName("PersonCache");
        personType.setKeyType(Integer.class);
        personType.setValueType(Person.class);
        // Specify the schema if applicable
        // personType.setDatabaseSchema("MY_DB_SCHEMA");
        personType.setDatabaseTable("Person");

        personType.setKeyFields(new JdbcTypeField(java.sql.Types.INTEGER, "id", Integer.class, "id"));

        JdbcTypeField idField = new JdbcTypeField(Types.INTEGER, "id", Integer.class, "id");
        JdbcTypeField orgIdField = new JdbcTypeField(Types.INTEGER, "orgId", Integer.class, "orgId");
        JdbcTypeField nameField = new JdbcTypeField(Types.VARCHAR, "name", String.class, "name");
        JdbcTypeField salaryField = new JdbcTypeField(Types.INTEGER, "salary", Integer.class, "salary");
        personType.setValueFields(idField, orgIdField, nameField, salaryField); // 网上大部分示例此处都只写了一个 id，就会导致 select 查询时数据不完整

        factory.setTypes(personType);

        personCacheCfg.setCacheStoreFactory(storeFactory);

        QueryEntity qryEntity = new QueryEntity();

        qryEntity.setKeyType(Integer.class.getName());
        qryEntity.setValueType(Person.class.getName());
        qryEntity.setKeyFieldName("id");

        Set<String> keyFields = new HashSet<>();
        keyFields.add("id");
        qryEntity.setKeyFields(keyFields);

        LinkedHashMap<String, String> fields = new LinkedHashMap<>();
        // 查询结果要显示的列
        fields.put("id", "java.lang.Integer");
        fields.put("orgId", "java.lang.Integer");
        fields.put("name", "java.lang.String");
        fields.put("salary", "java.lang.Integer");

        qryEntity.setFields(fields);

        personCacheCfg.setQueryEntities(Collections.singletonList(qryEntity));

        igniteCfg.setCacheConfiguration(personCacheCfg);
        igniteCfg.setMetricsLogFrequency(0);
        Ignite ignite = Ignition.start(igniteCfg);

        //IgniteCache<Integer, Person> personCache = ignite.cache("PersonCache");
        IgniteCache<Integer, Person> personCache = ignite.getOrCreateCache(personCacheCfg);
        personCache.loadCache(null);
        QueryCursor<List<?>> cursor = personCache.query(new SqlFieldsQuery("select * from Person"));
        System.out.println("1: " + cursor.getAll());


        /*for (int i = 0; i < 20000; i++) {
            //System.out.println("size: " + personCache.size());
            personCache.put(i, new Person(i, 100 + i, "zhouran_" + i, 10000 + i));
        }*/

        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            int i = 0;

            @Override
            public void run() {
                System.out.println("i: " + i + ", isEnabled: " + personCacheCfg.isWriteBehindEnabled());
                personCache.put(i, new Person(i, 100 + i, "zhouran_" + i, 10000 + i));
                System.out.println("putValue[" + i + "]: " + personCache.get(i) + "\n");
                /*System.out.println("cacheSize: " + personCache.size());
                personCache.put(personCache.size()+1, new Person(personCache.size()+1, 100 + i, "zhouran_" + i, 10000 + i));
                System.out.println("get[" + i + "]: " + personCache.get(i));*/
                i++;

            }
        }, 0, 1000, TimeUnit.MILLISECONDS);

        /*for (int i = 0; i < personCache.size() + 1; i++) {
            //System.out.println("get(" + i + "): " + personCache.get(i) + "\n");
            System.out.println(personCache.get(i));
        }*/

        personCache.loadCache(null);
        long startTime = System.currentTimeMillis();
        QueryCursor<List<?>> cursor2 = personCache.query(new SqlFieldsQuery("select * from Person"));
        System.out.println("2: " + cursor2.getAll() + ", 耗时: " + (System.currentTimeMillis() - startTime));

    }
}
