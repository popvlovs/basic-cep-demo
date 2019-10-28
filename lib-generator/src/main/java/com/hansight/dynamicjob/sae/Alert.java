package com.hansight.dynamicjob.sae;

import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Description: alert entity definition, for parse 'alert' field(type: map) purpose
 */
public class Alert {
    private boolean enabled;  //alarm enabled
    //private int focus; //focus enabled
    private String alarmContent;
    private int alarmLevel;
    //private String alarmType;
    private int alarmStage;

    //private boolean notificationEnabled; //alarm notification enabled
    //private String[] notificationConfig; //alarm notification id
    //private String[] alarmTag; //alarm tags
    private int ruleType;
    private String alarmAdvice;
    private Tags alarmTags;
    private String[] alarmKey = {"src_address", "dst_address", "alarm_content"};
    private Boolean alarmContentChanged;
    private Boolean alarmAdviceChanged;
    private String alarmAttckInfo;
    private List<Object> attckInfoList;


    /**
     * Response playbook ids, for Gartner-MQ presentation
     */
    private List<String> playbooks;

    public List<String> getPlaybooks() {
        return playbooks;
    }

    public void setPlaybooks(List<String> playbooks) {
        this.playbooks = playbooks;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public Boolean getAlarmContentChanged() {
        return alarmContentChanged;
    }

    public void setAlarmContentChanged(Boolean alarmContentChanged) {
        this.alarmContentChanged = alarmContentChanged;
    }

    public Boolean getAlarmAdviceChanged() {
        return alarmAdviceChanged;
    }

    public void setAlarmAdviceChanged(Boolean alarmAdviceChanged) {
        this.alarmAdviceChanged = alarmAdviceChanged;
    }

    public String getAlarmContent() {
        return alarmContent;
    }

    public void setAlarmContent(String alarmContent) {
        this.alarmContent = alarmContent;
    }

    public int getAlarmLevel() {
        return alarmLevel;
    }

    public void setAlarmLevel(int alarmLevel) {
        this.alarmLevel = alarmLevel;
    }

    public int getAlarmStage() {
        return alarmStage;
    }

    public void setAlarmStage(int alarmStage) {
        this.alarmStage = alarmStage;
    }

    /*public String getAlarmType() {
        return alarmType;
    }

    public void setAlarmType(String alarmType) {
        this.alarmType = alarmType;
    }

    public int getFocus() {
        return focus;
    }

    public void setFocus(int focus) {
        this.focus = focus;
    }

    public String[] getNotificationConfig() {
        return notificationConfig;
    }

    public void setNotificationConfig(String[] notificationConfig) {
        this.notificationConfig = notificationConfig;
    }

    public boolean isNotificationEnabled() {
        return notificationEnabled;
    }

    public void setNotificationEnabled(boolean notificationEnabled) {
        this.notificationEnabled = notificationEnabled;
    }

    public String[] getAlarmTag() {
        return alarmTag;
    }

    public String getStringAlarmTag() {
        if (alarmTag != null && alarmTag.length > 0) {
            return String.join(",", alarmTag);
        }
        return "";
    }

    public void setAlarmTag(String[] alarmTag) {
        this.alarmTag = alarmTag;
    }*/

    public int getRuleType() {
        return ruleType;
    }

    public void setRuleType(int ruleType) {
        this.ruleType = ruleType;
    }

    public String getAlarmAdvice() {
        return alarmAdvice;
    }

    public void setAlarmAdvice(String alarmAdvice) {
        this.alarmAdvice = alarmAdvice;
    }

    public Tags getAlarmTags() {
        return alarmTags;
    }

    public void setAlarmTags(Tags alarmTags) {
        this.alarmTags = alarmTags;
    }

    public String[] getAlarmKey() {
        return alarmKey;
    }

    public void setAlarmKey(String[] alarmKey) {
        this.alarmKey = alarmKey;
    }

    public String getAlarmAttckInfo() {
        return alarmAttckInfo;
    }

    public void setAlarmAttckInfo(String alarmAttckInfo) {
        this.alarmAttckInfo = alarmAttckInfo;
    }

    public List<Object> getAttckInfoList() {
        return attckInfoList;
    }

    public void setAttckInfoList(List<Object> attckInfoList) {
        this.attckInfoList = attckInfoList;
    }

    @Override
    public String toString() {
        return "Alert{" +
                "alarmContent='" + alarmContent + '\'' +
                ", enabled=" + enabled +
                //", focus=" + focus +
                ", alarmLevel=" + alarmLevel +
                //", alarmType='" + alarmType + '\'' +
                ", alarmStage=" + alarmStage +
                //", notificationEnabled=" + notificationEnabled +
                //", notificationConfig='" + notificationConfig + '\'' +
                ", ruleType='" + ruleType + '\'' +
                ", alarmAdvice='" + alarmAdvice + '\'' +
                ", alarmTags='" + alarmTags + '\'' +
                ", alarmAttckInfo='" + alarmAttckInfo + '\'' +
                ", alarmKey='" + StringUtils.join(alarmKey, "_") + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Alert)) return false;

        Alert alert = (Alert) o;

        if (enabled != alert.enabled) return false;
        //if (focus != alert.focus) return false;
        //if (notificationEnabled != alert.notificationEnabled) return false;
        if (!StringUtils.equals(alarmContent, alert.alarmContent)) return false;
        if (alarmLevel != alert.alarmLevel) return false;
        //if (!alarmType.equals(alert.alarmType)) return false;
        if (alarmStage != alert.alarmStage) return false;
        // Probably incorrect - comparing Object[] arrays with Arrays.equals
        //if (!Arrays.equals(notificationConfig, alert.notificationConfig)) return false;
        // Probably incorrect - comparing Object[] arrays with Arrays.equals
        //return Arrays.equals(alarmTag, alert.alarmTag);
        if (ruleType != alert.ruleType) return false;
        if (!StringUtils.equals(alarmAdvice, alert.alarmAdvice)) return false;
        if (!StringUtils.equals(alarmAttckInfo, alert.alarmAttckInfo)) return false;
        if (alarmTags != null ? !alarmTags.equals(alert.alarmTags) : alert.alarmTags != null) return false;
        String[] oldPlaybooks = Optional.ofNullable(alert.playbooks).orElse(Collections.emptyList()).toArray(new String[]{});
        Arrays.sort(oldPlaybooks);
        String[] newPlaybooks = Optional.ofNullable(playbooks).orElse(Collections.emptyList()).toArray(new String[]{});
        Arrays.sort(newPlaybooks);
        if (!Arrays.deepEquals(oldPlaybooks, newPlaybooks)) return false;
        return Arrays.equals(alarmKey, alert.alarmKey);
    }

    @Override
    public int hashCode() {
        int result = (enabled ? 1 : 0);
        //result = 31 * result + focus;
        result = 31 * result + (alarmContent != null ? alarmContent.hashCode() : 0);
        result = 31 * result + alarmLevel;
        //result = 31 * result + alarmType.hashCode();
        result = 31 * result + alarmStage;
        //result = 31 * result + (notificationEnabled ? 1 : 0);
        //result = 31 * result + Arrays.hashCode(notificationConfig);
        //result = 31 * result + Arrays.hashCode(alarmTag);
        result = 31 * result + ruleType;
        result = 31 * result + (alarmAdvice != null ? alarmAdvice.hashCode() : 0);
        result = 31 * result + (alarmAttckInfo != null ? alarmAttckInfo.hashCode() : 0);
        result = 31 * result + (alarmTags != null ? alarmTags.hashCode() : 0);
        result = 31 * result + Arrays.hashCode(alarmKey);
        return result;
    }
}
