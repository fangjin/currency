import math

def calZscore(scores,currDiff):
    sumScores = sum(scores)
    if len(scores) == 0:
        return 0.0
    meanScore = sumScores/len(scores)
    
    stdDev = calSD(scores)
    if stdDev == 0.0:
        return 0.0
    else:
        return (currDiff - meanScore)/stdDev

    "scores is a list [one_day_change, not raw currency]"
def calSD(scores):
    sumScores = sum(scores)
    meanScore = sumScores/len(scores)
    squareSum = 0.0
    for score in scores:
        squareSum = squareSum + math.pow(score-meanScore,2)
    stdDev = 0.0
    if len(scores) == 1:
        stdDev = 0.0
    else:
        stdDev = math.sqrt( squareSum/(len(scores)-1)) 
    print stdDev
    return stdDev
    

if __name__ == "__main__":
    print "zcore is: %f" % calZscore([1,3,2],0.5)
    

