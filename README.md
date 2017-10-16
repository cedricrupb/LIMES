# LIMES
Repository for LIMES - Link Discovery Framework for Metric Spaces.
=======

[![Build Status](https://travis-ci.org/dice-group/LIMES.svg?branch=master)](https://travis-ci.org/dice-group/LIMES)

Development branch for LIMES - Link Discovery Framework for Metric Spaces.

## Generating Jar File (Headless)
installing use:
```
mvn clean install
```

Creating the runnable jar file including the dependencies use:
```
mvn clean package shade:shade -Dcheckstyle.skip=true -Dmaven.test.skip=true
```

The .jar will be placed in `limes-core/target/limes-core-VERSION-SNAPSHOT.jar`

## Generating Jar File (GUI)
switch to `limes-gui` and use:
```
mvn jfx:jar -Dcheckstyle.skip=true -Dmaven.test.skip=true
```

The .jar will be placed in `limes-gui/target/jfx/app/limes-GUI-VERSION-SNAPSHOT-jfx.jar`

### Importing into Eclipse
In case Eclipse does not recognize the project as Java. Please run the following from the `limes-core/` directory:
```
mvn eclipse:eclipse
```
Then, update the project on Eclipse.


## More details

* [Project web site](http://cs.uni-paderborn.de/ds/research/research-projects/active-projects/limes/)
* [User manual](http://dice-group.github.io/LIMES/user_manual/)
* [Developer manual](http://dice-group.github.io/LIMES/developer_manual/)

