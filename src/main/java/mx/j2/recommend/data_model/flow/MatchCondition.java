package mx.j2.recommend.data_model.flow;

import mx.j2.recommend.thrift.Request;
import mx.j2.recommend.util.DefineTool;

public class MatchCondition {
    private boolean isNeedPlatformMatch;
    private String platfromIndex;
    public MatchCondition() {
        isNeedPlatformMatch = false;
        platfromIndex = "DEFAULT";
    }
    public MatchCondition(MatchCondition other){
        this.isNeedPlatformMatch = other.isNeedPlatformMatch;
        this.platfromIndex = other.platfromIndex;
    }

    public void setPlatfromID(String platfrom){
        for(DefineTool.PlatformsEnum platformsEnum: DefineTool.PlatformsEnum.values()){
            if(platformsEnum.getName().equals(platfrom)){
                this.platfromIndex = platformsEnum.getIndex();
                isNeedPlatformMatch = true;
            }
        }
    }

    public String getMatchConditionKey(){
        return getPlatfromIndexString();
    }

    private String getPlatfromIndexString(){
        return platfromIndex;
    }

    public boolean isRequestMatch(Request request){
        boolean isMatch = true;
        if(isNeedPlatformMatch){
            String platformId = request.platformId;
            if(!this.platfromIndex.equals(platformId)){
                isMatch = false;
            }
        }
        return isMatch;
    }

    public int getMatchWeight(){
        int count = 0;
        if(isNeedPlatformMatch){
            count++;
        }
        return count;
    }

    @Override
    public String toString(){
        return platfromIndex;
    }
}
