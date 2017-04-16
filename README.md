# DS410 - Project

Compile: 

```
> sbt package
```

Usage: 

```
> spark-submit [spark options] milestone1.jar
```

# Milestone 1 
* Use k-means clustering to form occupation nodes into clusters. There will be 4 clusters, each being a quartile of the average/median salaries.

# Milestone 2
 * Develop algorithm to find the similarity measures of each occupation to each other using traits such as average annual salary and job industry group. This will be used to see how the similarity between occupations have changed throughout the years. For example, statisticians and software developers may have been less similar in the past, but as time goes on they may have becomes more similar (as data science becomes more of a trend).  This may be changed as finding the similarity measures for each combination can be costly when working with big data.

# Milestone 3
 * Use linear regression to predict the salary change of each occupation based on the data set. Our label attribute for regression will be the average annual salary for each occupation. Our regular attribute will be the years (2007 onwards). These regression lines will be formed for each occupation to predict their annual average annual salary for the coming years. The year 2016 can be used to compare our results if that data is readily available. The source http://www1.salary.com can be used to compare our results. This lab may also be changed depending on what is learned in future lectures and how it can be applied to our project.
