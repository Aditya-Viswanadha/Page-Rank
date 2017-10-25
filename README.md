 # Page-Rank
Implementation og Google Page Rank


The following are the steps used to excute various parts of this project.
First we load the input files into the hadoop file system.
Then we execute these for Page Rank.

The project is run on two different input files:
1. wiki-micro.txt
2. simplewiki-20150901-pages-articles-processed.xml

I implemented the project in 3 different classes. 



# Class 1, 

This process individual lines of the input corpus, one at a time.
The patterns are matched so as to extract the information form the title and the text part of the xml file by using the title pattern and text pattern.

# Class 2,

This class implements the page rank calculations.
First the calculations are calculated using intial values but are later modified in later iterations.
The max limit to the iterations are set at 20 which can be changed. 
The values i=of the page rank might reach a steady state. I am checking them by comparing the immediate output files of the iterations. If both the files are same, we can it reached a steady state.


# Class 3,

This part takes the output of the last iteration and arranges the results according to the deccreasing order of the page rank.


# Steps for execution:

1. Right click on the project.
2. Click on the Run As configuration.
3. Set the arguments for the project for input and output.
4. Select the main class for the project as class1 class.
5. Run the project.
6. Go to the output location to find th output folder with the output file.
7. Repeat the same steps with the next input file.









