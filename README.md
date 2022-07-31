# DE-Project

## Overview of the dataset
- This is a historical dataset on the modern Olympic Games, including all the Games from Athens 1896 to Rio 2016 from Kaggle.

- Note that the Winter and Summer Games were held in the same year up until 1992. After that, they staggered them such that Winter Games occur on a four year cycle starting with 1994, then Summer in 1996, then Winter in 1998, and so on. A common mistake people make when analyzing this data is to assume that the Summer and Winter Games have always been staggered.

### Column Content:
- ID - Unique number for each athlete
- Name - Athlete's name
- Sex - M or F
- Age - Integer
- Height - In centimeters
- Weight - In kilograms
- Team - Team name
- NOC - National Olympic Committee 3-letter code
- Games - Year and season
- Year - Integer
- Season - Summer or Winter
- City - Host city
- Sport - Sport
- Event - Event
- Medal - Gold, Silver, Bronze, or NA

## Overview of the project goals and the motivation for it.
- The goal of the project is to go through the complete data engineering process to answer questions we have about the topic in the dataset. In the first Milestone we will do the following: Explore the data, Clean the data, handle missing data, plot data distributions and find research questions.

## Descriptive steps used for the work done in this milestone.
- First we saw the describtion of the dataset to get fimiliar with the given dataset and we plotted the number of male and femal participants to see a nice comparison between both. Then we went to handle the outliers of the Age, Height and Weight. Their outliers were not false data entries or irregular values so we decided to leave them. Furthermore, there were a lot of NAN values in these columns so we used multivariant and single variant impuation to handle them. Then we came up with research questions and tried to answer them using different plots.


## Data exploration questions
1. Countries that won most medals ?   : Using medalist integrated dataframe
2. Countries that won most medals in summer olympics?
3. Countries that won most medals in winter olympics?
^^ all of the above questions again but in order of Gold medals^^
4. For the number one country in summer games what is the sport that gianed it the most medals?
- Top countries in that sport?
5. Is there a relation between this sport and the players average height in the top teams?
- Is there a relation between this sport and the players average height across all teams?
6. For the number one country in winter games what is the sport that gained it the most medals?
- Top countries in that sport?
7. Is there a relation between this sport and players average BMI for the top teams?  :Using BMI Column
- Is there a relation between this sport and players average BMI for all teams?     : Using BMI column
8. Athletes that brought the most medals to their teams? : Using All time total column
9. for the top 10 athletes that participated the most times across history how many times or how well did they preform according to the number of medals gained across time?

## Description of the features you added
- We added BMI column for each athlete using their weight and height. We also added All Time Total column for each athlete which represents the total number of medals he won across history. We used both columns in questions 7&8
