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
 * Apply the k-means clustering across all of the datasets. Use persist and cache to increase performance. Compare and contrast with and without partition/cache.

# Milestone 3
 * Use linear regression to predict the salary change of each occupation based on the data set. 
 * The data set we form up to do the linear regression would be in the following SVM format:
  * \<avg_sal> 1:\<year> 
  * The **year** will be the years (2007 onwards). 
  * The **avg_sal** will be the **average annual salary for each occupation**. 
 
  * These regression lines will be formed for each occupation to predict their annual average annual salary for the coming years. 
 * The year 2016 can be used to compare our results if that data is readily available. The source http://www1.salary.com can be used to compare our results. 
