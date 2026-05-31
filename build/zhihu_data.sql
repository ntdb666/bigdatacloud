/*
 Navicat Premium Data Transfer

 Source Server         : 玉岩服务器mysql
 Source Server Type    : MySQL
 Source Server Version : 50744
 Source Host           : 193.112.128.232:3306
 Source Schema         : cloudlocaltest

 Target Server Type    : MySQL
 Target Server Version : 50744
 File Encoding         : 65001

 Date: 31/05/2026 15:36:55
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for zhihu_data
-- ----------------------------
DROP TABLE IF EXISTS `zhihu_data`;
CREATE TABLE `zhihu_data`  (
  `name` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL COMMENT '????',
  `url_token` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL COMMENT '????ID/????',
  `gender` tinyint(4) NULL DEFAULT NULL COMMENT '??: 1?, 0?, -1??',
  `user_type` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL COMMENT '????: people/organization',
  `avatar_url` text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL COMMENT '????',
  `headline` text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL COMMENT '?????',
  `answer_count` int(11) NULL DEFAULT 0 COMMENT '???',
  `articles_count` int(11) NULL DEFAULT 0 COMMENT '???',
  `follower_count` bigint(20) NULL DEFAULT 0 COMMENT '???',
  `following_count` int(11) NULL DEFAULT 0 COMMENT '???',
  `voteup_count` bigint(20) NULL DEFAULT 0 COMMENT '???',
  `thanked_count` bigint(20) NULL DEFAULT 0 COMMENT '???',
  `favorited_count` bigint(20) NULL DEFAULT 0 COMMENT '????',
  `description` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL COMMENT '??????(?HTML)',
  `business` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL COMMENT '??/????JSON',
  `locations` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL COMMENT '???????JSON(???????)',
  `educations` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL COMMENT '????JSON(???????)',
  `employments` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL COMMENT '????JSON(???????)'
) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci ROW_FORMAT = Dynamic;

SET FOREIGN_KEY_CHECKS = 1;
