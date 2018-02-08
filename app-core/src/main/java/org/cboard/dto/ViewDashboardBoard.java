package org.cboard.dto;

import com.alibaba.fastjson.JSONObject;
import org.cboard.pojo.DashboardBoard;
import com.google.common.base.Function;
import org.cboard.services.role.RolePermission;

import javax.annotation.Nullable;
import java.util.Map;

/**
 * Created by yfyuan on 2016/8/23.
 */
public class ViewDashboardBoard {

    private Long id;
    private String userId;
    private int folderId;
    private String folderName;
    private int folderIsPrivate;
    private String name;
    private String userName;
    private String loginName;
    private String createTime;
    private String updateTime;
    private Map<String, Object> layout;
    private String categoryName;
    private boolean edit;
    private boolean delete;
    private Integer orderNum;

    public static final Function TO = new Function<DashboardBoard, ViewDashboardBoard>() {
        @Nullable
        @Override
        public ViewDashboardBoard apply(@Nullable DashboardBoard input) {
            return new ViewDashboardBoard(input);
        }
    };

    public ViewDashboardBoard(DashboardBoard board) {
        this.id = board.getId();
        this.userId = board.getUserId();
        this.folderId = board.getFolderId();
        this.orderNum = board.getOrderNum();
        this.folderName = board.getFolderName();
        this.folderIsPrivate = board.getFolderIsPrivate();
        this.name = board.getName();
        this.userName = board.getUserName();
        this.loginName = board.getLoginName();
        this.createTime = board.getCreateTime().toString();
        this.updateTime = board.getUpdateTime().toString();
        this.layout = JSONObject.parseObject(board.getLayout());
        this.categoryName = board.getCategoryName();
        this.edit = RolePermission.isEdit(board.getPermission());
        this.delete = RolePermission.isDelete(board.getPermission());
    }

    public boolean isEdit() {
        return edit;
    }

    public void setEdit(boolean edit) {
        this.edit = edit;
    }

    public boolean isDelete() {
        return delete;
    }

    public void setDelete(boolean delete) {
        this.delete = delete;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getLoginName() {
        return loginName;
    }

    public void setLoginName(String loginName) {
        this.loginName = loginName;
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }

    public String getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(String updateTime) {
        this.updateTime = updateTime;
    }

    public Map<String, Object> getLayout() {
        return layout;
    }

    public void setLayout(Map<String, Object> layout) {
        this.layout = layout;
    }

    public int getFolderId() {
        return folderId;
    }

    public void setFolderId(int folderId) {
        this.folderId = folderId;
    }

    public String getFolderName() {
        return folderName;
    }

    public Integer getOrderNum() {
		return orderNum;
	}

	public void setOrderNum(Integer orderNum) {
		this.orderNum = orderNum;
	}

	public void setFolderName(String folderName) {
        this.folderName = folderName;
    }

    public int getFolderIsPrivate() {
        return folderIsPrivate;
    }

    public void setFolderIsPrivate(int folderIsPrivate) {
        this.folderIsPrivate = folderIsPrivate;
    }

    public String getCategoryName() {
        return categoryName;
    }

    public void setCategoryName(String categoryName) {
        this.categoryName = categoryName;
    }
}
