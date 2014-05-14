Dropwizard Cassandra
================

*For working with Cassandra in a Dropwizard application*

Usage
-----

Dropwizard Cassandra is published to [Maven Central](http://search.maven.org/#search|ga|1|g%3Acom.datasift.dropwizard), 
so just add the module(s) you wish to use to your `pom.xml`:

```xml
<dependencies>
    <dependency>
        <groupId>com.datasift.dropwizard</groupId>
        <artifactId>dropwizard-cassandra</artifactId>
        <version>0.7.0-1</version>
    </dependency>
</dependencies>
```

Or whatever you need to do to make SBT/Gradle/Ivy/Buildr/etc. happy.

In your `Configuration` class, just add a `CassandraClusterFactory` member for
configuration of the cluster connection:

```java
class MyConfiguration extends Configuration {

    @NotNull
    @Valid
    private CassandraClusterFactory cassandra = new CassandraClusterFactory();

    @JsonProperty("cassandra")
    public CassandraClusterFactory getCassandraClusterFactory() {
        return cassandra;
    }

    @JsonProperty("cassandra")
    public void setCassandraClusterFactory(CassandraClusterFactory factory) {
        this.cassandra = factory;
    }
}
```

Finally, in your `Application` class, build an instance of the client:

```java
class MyApplication extends Application<MyConfiguration> {

    @Override
    public void run(MyConfiguration configuration, Environment environment) {
        Cluster client = configuration
                .getCassandraClusterFactory()
                .build(environment);
    }
}
```

`Cluster` instances will be configured as defined by the 
`CassandraClusterFactory` and will automatically be managed as part of the 
application lifecycle.

Versioning
----------

Dropwizard Cassandra is versioned in lock-step with upstream Dropwizard. The 
versioning scheme for Dropwizard Cassandra is as follows:

    ${dropwizard.version}-{release.number}

The "release number" signifies the differences between two builds of Dropwizard 
Cassandra that are built against the same upstream version of Dropwizard.

The practical consequence of this is that an upgrade of Dropwizard Cassandra 
will often require an upgrade of Dropwizard itself, however, this is always 
clearly indicated by the version number of Dropwizard Cassandra itself.

License
-------

This software is licensed under the [Apache License Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)

