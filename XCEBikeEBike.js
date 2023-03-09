const db = require('../nodealmvc/aldb/index');
const { date } = require('../nodealmvc/phpfuc/datetime');

/**
 * 车辆DAO，车辆imei绑定相关
 */

const Sequelize = require('sequelize');
const XCEBikeBizData = require('./XCEBikeBizData');
const { XCVoltageProgram } = require('./voltageProgram');


let xcSequelize = null;
let xcRedis = null;
let CarInfoModelOperateDAO = null;
let BindInfoModelOperateDAO = null;
/** redis Key */
/** 设备与代理商的绑定关系 */
const DeviceBindingAgentKey = 'imeiAgentBinding_';
// 根据carId获取imei的KEY
const Car2ImeiKey = 'carImeiBindings_';
// 根据imei获取carId的KEY
const Imei2CarKey = 'imeiCarBindings_';
// 根据IMEI绑定电压方案
const imeiVoltageProgram = 'xc_battery_Name_';
// 记录车辆开锁失败次数和类型
const unlockFailInfo = 'unlockFailInfo';

// 根据carId绑定头盔
const carId2helmet = 'helmet_';
// 头盔报修标志位
const repair2helmet = 'repair_helmet_flag_';
// 头盔锁定标志位
const lock2helmet = 'lock_helmet_flag_';
// 头盔佩戴标志位
const helmetWare = 'helmet_ware_flag_';

const CREATE_RULE = { freezeTableName: true };
// 车辆出库信息表
const XC_EBIKE_DEVICE_CAR_INFO_DS = {
  carId: {
    type: Sequelize.INTEGER(9),
    primaryKey: true,
    autoIncrement: true
  },
  carNo: {
    type: Sequelize.STRING(16),
    unique: true
  },
  brand: {
    type: Sequelize.STRING(16),
  },
  model: {
    type: Sequelize.STRING(16),
  },
  area: {
    type: Sequelize.STRING(32),
  },
  voltageProgramId: {
    type: Sequelize.INTEGER(32)
  },
  isHelmet: {
    type: Sequelize.INTEGER(1),
    allowNull: true,
    defaultValue: 0,
    comment: '是否配置了头盔 1 是 0 否'
    },
  investor: {
    type: Sequelize.STRING(32),
    defaultValue: '',
    comment: '资方代码'
  }
};

// 车辆绑定关系表
const XC_EBIKE_DEVICE_BINDING_TABLE_DS = {
  carId: {
    type: Sequelize.STRING(9),
    allowNull: false
  },
  imei: {
    type: Sequelize.STRING(16),
    allowNull: false
  }
};

class XCEBikeEBike {
  /**
     * 从json初始化
     * @param {object} json 
     */
  constructor(json) {
    this.agentId = 2;
    this.imei = json.imei;
  }

  static configRedis(redisClient, redisSubClient) {
    xcRedis = redisClient;
  }

  /**
     * 配置数据库实例
     */
  static configSequelize(sequelizeInstance) {
    xcSequelize = sequelizeInstance;
    CarInfoModelOperateDAO = xcSequelize.define('xc_ebike_2_carInfo', XC_EBIKE_DEVICE_CAR_INFO_DS, {
      freezeTableName: CREATE_RULE.freezeTableName,
      initialAutoIncrement: 100600000
    });
    BindInfoModelOperateDAO = xcSequelize.define('xc_ebike_2_bindingInfo', XC_EBIKE_DEVICE_BINDING_TABLE_DS, {
      freezeTableName: CREATE_RULE.freezeTableName,
      paranoid: true
    });
  }

  // 获取车辆表的操作DAO
  async deviceCarInfoDAO() {
    return CarInfoModelOperateDAO;
  }
  
  // 获取设备绑定表的操作DAO
  async deviceBindingInfoDAO() {
    return BindInfoModelOperateDAO;
  }

  // 获取车辆表的操作DAO
  static get carInfoOperateDAO() {
    return CarInfoModelOperateDAO;
  }
  
  // 获取设备绑定表的操作DAO
  static get bindInfoOperateDAO() {
    return BindInfoModelOperateDAO;
  }

  /**
     * 由车辆信息数组存上传结果
     * @param {array} carInfoList 
     */
  async uploadCarId(carInfoList) {
    let model = await this.deviceCarInfoDAO();
    let result = await model.bulkCreate(carInfoList);

    return result;
  }

  /**
     * 刷新自增id的起始值，保证carId连续
     */
  async refreshAutoIncrement() {
    let model = await this.deviceCarInfoDAO();
    let maxCarId = await model.max('carId');
    maxCarId += 1;
    let ret = await xcSequelize.query(`ALTER TABLE xc_ebike_2_carInfo auto_increment=${maxCarId};`);

    return ret;
  }

  /**
   * 根据条件查询车辆信息
   * @param {*} query 
   */
  async findByQueryInfo(query) {
    let model = await this.deviceCarInfoDAO();
    let ret = await model.findAll({
      where: query
    });
    return ret;
  }

  /**
   * 根据条件查询车辆
   * @param {*} page 
   * @param {*} size 
   * @param {*} query 
   * @param {*} voltageProgramId 
   * @param {*} serviceId 
   */
  async findByquery(page, size, query) {
    let Sql = `SELECT a.imei,b.name,c.carId ,c.carNo , c.brand ,c.model ,c.area , c.createdAt ,c.voltageProgramId,a.serviceId,e.name as programName
    FROM xc_ebike_2_carInfo AS c 
    LEFT JOIN xc_voltage_program AS e ON c.voltageProgramId=e.id 
    LEFT JOIN xc_ebike_2_bindingInfo AS d ON c.carId =d.carId
    LEFT JOIN xc_ebike_2_devices AS a ON a.imei =d.imei 
    LEFT JOIN xc_ebike_gfence_2 AS b ON b.id =a.serviceId`;
    let SqlCount = `SELECT count(*) as count
    FROM xc_ebike_2_carInfo AS c 
    LEFT JOIN xc_voltage_program AS e ON c.voltageProgramId=e.id 
    LEFT JOIN xc_ebike_2_bindingInfo AS d ON c.carId =d.carId
    LEFT JOIN xc_ebike_2_devices AS a ON a.imei =d.imei 
    LEFT JOIN xc_ebike_gfence_2 AS b ON b.id =a.serviceId`;
    if (query) {
      let sqlWhere = ' ';
      if (query.carId) {
        sqlWhere += ` c.carId=${query.carId} and`;
      }
      if (query.carNo) {
        sqlWhere += ` c.carNo=${query.carNo} and`;
      }
      if (query.brand) {
        sqlWhere += ` c.brand=${query.brand} and`;
      }
      if (query.model) {
        sqlWhere += ` c.model=${query.model} and`;
      }
      if (query.area) {
        sqlWhere += ` c.area=${query.area} and`;
      }
      if (query.startTime && query.endTime) {
        sqlWhere += ` (c.createdAt between '${query.startTime}' and '${query.endTime}') and`;
      }
      if (query.voltageProgramId == 'notOn') {
        sqlWhere += ' c.voltageProgramId IS NULL and';
      } else if (query.voltageProgramId) {
        sqlWhere += ` c.voltageProgramId=${query.voltageProgramId} and`;
      }
      if (query.serviceId == 'notOn') {
        sqlWhere += ' a.serviceId IS NULL and';
      } else if (query.serviceId) {
        sqlWhere += ` a.serviceId=${query.serviceId} and`;
      }
      // 截取最后三个字符，判断如果是and,去掉最后的and
      let last = sqlWhere.substring(sqlWhere.length - 3);
      if (last == 'and') {
        sqlWhere = sqlWhere.slice(0, sqlWhere.length - 3);
        sqlWhere = ` where ${sqlWhere}`;
      }
      SqlCount += sqlWhere;
      sqlWhere += ` ORDER BY c.carId ASC LIMIT ${page * size},${size}`;
      Sql += sqlWhere;
    } else {
      Sql += ` ORDER BY c.carId ASC LIMIT ${page * size},${size}`;
    }
    let res = await xcSequelize.query(Sql, { type: xcSequelize.QueryTypes.SELECT });
    let resCout = await xcSequelize.query(SqlCount, { type: xcSequelize.QueryTypes.SELECT });
    if (res) {
      return { 
        rows: res,
        count: resCout[0].count
      };
    }
    return { 
      rows: [],
      count: 0
    };
  }

  /**
   * 修改车辆绑定的电压方案
   * @param {*} carId 
   * @param {*} id 
   */
  async updateVoltageProgram(carId, id) {
    let vp = new XCVoltageProgram();
    let voProgram = await vp.findOneName(id);
    let model = await this.deviceCarInfoDAO();
    let res = await model.update({
      voltageProgramId: id
    }, {
      where: {
        carId
      }
    });
    // 车辆与电压方案绑定存Redis
    if (voProgram) {
      let info = JSON.stringify(voProgram);
      await xcRedis.set(`${imeiVoltageProgram}${carId}`, info);
    }
    return res;
  }

  /**
   * 车辆绑定电压方案
   * @param {*} carId 
   * @param {*} id 
   */
  async bingdingProgram(carId, id) {
    let vp = new XCVoltageProgram();
    let voProgram = await vp.findOneName(id);
    // 车辆与电压方案绑定存Redis
    let res = null;
    if (voProgram) {
      let info = JSON.stringify(voProgram);
      res = await xcRedis.set(`${imeiVoltageProgram}${carId}`, info);
    } else { // 若果没有查到方案，则使用默认方案
      res = await xcRedis.del(`${imeiVoltageProgram}${carId}`);
    }
    return res;
  }

  /**
   *  车辆绑定头盔
   *  目前头盔
   */
  static async bindHelmet(carId) {
    return xcRedis.set(`${carId2helmet}${carId}`, carId);
  }

  /**
   *  车辆解绑头盔
   *  目前头盔
   */
  static async unBindHelmet(carId) {
    return xcRedis.del(`${carId2helmet}${carId}`);
  }

  /**
   * 根据carId获取头盔标志位
   */
  static async getHelmetByCarId(carId) {
    return xcRedis.get(`${carId2helmet}${carId}`);
  }

  /**
   * 设置头盔锁定标志位
   */
  static async setHelmetLock(carId) {
    return xcRedis.set(`${lock2helmet}${carId}`, carId);
  }

  /**
   * 获取头盔锁定标志位
   */
  static async getHelmetLock(carId) {
    return xcRedis.get(`${lock2helmet}${carId}`);
  }
  
  /**
   * 删除头盔锁定标志位
   */
  static async delHelmetLock(carId) {
    return xcRedis.del(`${lock2helmet}${carId}`);
  }

  /**
   * 设置头盔报修标志位
   */
  static async setHelmetRepair(carId, userId) {
    return xcRedis.set(`${repair2helmet}${carId}_${userId}`, 1, 'EX', 30 * 60);
  }

  /**
   * 获取头盔报修标志位
   */
  static async getHelmetRepair(carId, userId) {
    return xcRedis.get(`${repair2helmet}${carId}_${userId}`);
  }

  /**
   * 删除头盔报修标志位
   */
  static async delHelmetRepair(carId, userId) {
    return xcRedis.del(`${repair2helmet}${carId}_${userId}`);
  }

  /**
    * 设置头盔锁定佩戴标志位
    */
  static async setHelmetWare(carId) {
    return xcRedis.set(`${helmetWare}${carId}`, carId);
  }
     
  /**
        * 获取头盔锁定标志位
        */
  static async getHelmetWare(carId) {
    return xcRedis.get(`${helmetWare}${carId}`);
  }
       
  /**
        * 删除头盔锁定标志位
        */
  static async delHelmetWare(carId) {
    return xcRedis.del(`${helmetWare}${carId}`);
  }
  

  /**
   * 根据IMEI查询电压方案
   * @param {*} imei 
   */
  static async getProgramByImei(imei) {
    // 根据IMEI查询carId
    let carId = await XCEBikeEBike.carIdByImei(imei);
    let res = await xcRedis.get(`${imeiVoltageProgram}${carId}`);
    return res;
  }

  /**
   * 记录车辆的开锁失败次数和类型
   * @param {*} carId 车辆ID
   * @param {*} failNum 失败次数
   * @param {*} failType 失败类型
   */
  static async setOrUpdateCarUnlockFailInfo(carId, failNum, failType) {
    const data = {
      failNum,
      failType,
    };
    const result = await xcRedis.set(`${unlockFailInfo}_${carId}`, JSON.stringify(data));
    return result;
  }

  /**
   * 获取车辆的开锁失败次数和类型
   * @param {*} carId 车辆ID
   */
  static async getCarUnlockFailInfo(carId) {
    let result = await xcRedis.get(`${unlockFailInfo}_${carId}`);
    if (result) {
      result = JSON.parse(result);
    }
    return result;
  }

  /**
   * 根据carId查询电压方案
   * @param {*} imei 
   */
  static async getProgramByCarId(carId) {
    let res = await xcRedis.get(`${imeiVoltageProgram}${carId}`);
    return res;
  }

  /**
     * 获取一条carId信息
     * @param {*} carId 
     */
  async getOneCarId(carId) {
    let model = await this.deviceCarInfoDAO();
    let ret = await model.findOne({
      where: {
        carId
      }
    });
    return ret;
  }

  /**
     * 分页获取carId
     * @param {*} query 查询条件
     * @param {*} countPerpage 
     * @param {*} currPage 
     */
  async getcarIdByPageSize(countPerpage, currPage, query) {
    const model = await this.deviceCarInfoDAO();
    const total = await model.count({
      where: query
    });
    const ret = await model.findAll({
      where: query,
      limit: countPerpage,
      offset: countPerpage * currPage,
      order: [['carId', 'DESC']]
    });
    return { total, list: ret };
  }
    
  /**
     * 批量更新车辆信息
     * @param {array} carInfoList 
     */
  async updateCarInfo(carInfoList) {
    const model = await this.deviceCarInfoDAO();
    await Promise.all(carInfoList.map(async (info) => {
      await model.update();
    }));
  }

  /**
     * 获取所有车辆信息
     */
  async findAllCarInfo() {
    let model = await this.deviceCarInfoDAO();
    let allDevices = await model.findAll();
    return allDevices;
  }

  /**
     * 根据时间获取车辆信息
     * @param {*} time 
     */
  async findCarByTime(timeStart, timeEnd) {
    let model = await this.deviceCarInfoDAO();

    let allDevices = await model.findAll({
      where: {
        createdAt: {
          $gt: timeStart,
          $lt: timeEnd
        }
      }
    });

    return allDevices;
  }

  /**
     * 设备投入使用。调用于设备与车辆绑定时
     * 此时Redis里还没有设备及设备与代理商对应记录
     * 计入管理的设备总数
     * 完成设备与代理商的绑定关系，当前写死为2
     * @param {*} agentId 
     * @param {*} imei 
     */
  static async comeIntoServiceRedis(agentId, imei) {
    agentId = 2;
    let ret = await xcRedis.set(`${DeviceBindingAgentKey}${imei}`, agentId);
    ret = await XCEBikeBizData.incrAgentDeviceCount(agentId, imei);
    return ret;
  }

  /**
     * 设备与车辆解除绑定时调用
     * @param {*} agentId 
     * @param {*} imei 
     */
  static async outOfServiceRedis(agentId, imei) {
    await xcRedis.del(`${DeviceBindingAgentKey}${imei}`);
    await XCEBikeBizData.decrAgentDeviceCount(agentId, imei);
  }

  /**
     * 根据carId从数据库中获取绑定信息
     * @param {*} carId
     * @param {*} agentId
     */
  async imeiByCarIdDAO(carId, agentId) {
    let model = await this.deviceBindingInfoDAO(agentId);
    let result = await model.findOne({
      where: {
        carId
      }
    });
    return result ? result.imei : result;
  }

  // 根据车辆号,获取绑定信息
  async findBindInfoByQuery(query = {}) {
    const bindModel = await this.deviceBindingInfoDAO();
    const result = await bindModel.findOne({
      where: query,
      rows: true
    });
    return result; 
  }

  /**
     * 根据imei从数据库中获取绑定信息
     * @param {*} imei
     * @param {*} agentId
     */
  async carIdByimeiDAO(imei, agentId) {
    let model = await this.deviceBindingInfoDAO(agentId);
    let result = await model.findOne({
      where: {
        imei
      }
    });
    return result ? result.carId : result;
  }

  /**
     * 查询carId和imei的绑定关系，若已相互绑定返回绑定信息，若均可被绑定返回true，否则返回false
     * @param {*} carId
     * @param {*} imei
     * @param {*} agentId
     */
  async bindState(carId, imei, agentId) {
    carId = String(carId);
    imei = String(imei);
    let carIdBindImei = await this.imeiByCarIdDAO(carId, agentId);
    let imeiBindCarId = await this.carIdByimeiDAO(imei, agentId);

    if (!carIdBindImei && !imeiBindCarId) {
      return true;
    } if (carIdBindImei == imei && imeiBindCarId == carId) {
      return {
        imei,
        carId
      };
    } if (carIdBindImei != null && carIdBindImei != imei) {
      throw `${carId} 已经被绑定了`;
    } else if (imeiBindCarId != null && imeiBindCarId != carId) {
      throw `${imei} 已经被绑定了`;
    } else return false;
  }

  /**
     * 设备绑定一辆车
     * 同时往车-imei表中记录一条记录
     * 往数据库中记录一条信息
     * carId agentId
     * @param {*} carId 
     * @param {*} agentId
     */
  async bindCarId(carId, agentId) {
    let model = await this.deviceBindingInfoDAO(agentId);
    let ret = await this.bindState(carId, this.imei, agentId);
    if (ret === false) {
      return false;
    } if (ret === true) {
      ret = await model.findOrCreate({
        where: {
          carId,
          imei: this.imei,
        },
        defaults: {
          carId,
          imei: this.imei
        }
      });
    }

    if (!ret) {
      return false;
    } 
    ret = await xcRedis.set(`${Imei2CarKey}${this.imei}`, carId);
    ret = await xcRedis.set(`${Car2ImeiKey}${carId}`, this.imei);
    await XCEBikeEBike.comeIntoServiceRedis(agentId, this.imei);
        
    return ret;
  }

  /**
     * 解绑车-imei
     * 同时解绑imei-车
     * 对数据库内数据进行删除
     * carId agentId
     * @param {*} carId 
     * @param {*} agentId
     */
  async unBindCarId(carId, agentId) {
    let model = await this.deviceBindingInfoDAO(agentId);
    let ret = await this.bindState(carId, this.imei, agentId);

    if (ret === true || ret === false) {
      return false;
    } 
    ret = await model.destroy({
      where: {
        carId
      }
    });
        

    if (!ret) {
      return false;
    } 
    ret = await xcRedis.del(`${Imei2CarKey}${this.imei}`);
    ret = await xcRedis.del(`${Car2ImeiKey}${carId}`);
    await XCEBikeEBike.outOfServiceRedis(agentId, this.imei);
        
    return ret;
  }

  /**
     * 返回设备绑定的车Id
     */
  async bindedCarId() {
    let ret = await xcRedis.get(`${Imei2CarKey}${this.imei}`);
    return ret;
  }

  /**
     * 给定carId查找绑定的设备imei
     * @param {*} carId 
     */
  static async imeiByCarId(carId) {
    let ret = await xcRedis.get(`${Car2ImeiKey}${carId}`);
    return ret;
  }
        
  /**
     * 返回设备绑定的车Id
     */
  static async carIdByImei(imei) {
    let ret = await xcRedis.get(`${Imei2CarKey}${imei}`);
    return ret;
  }

  /**
     * 根据车牌号获取车辆Id
     * @param {*} carNo
     */
  async carIdByCarNo(carNo) {
    const model = await this.deviceCarInfoDAO();
    const ret = model.findOne({
      where: {
        carNo
      }
    });
    return ret;
  }

  /**
     * 分页获取carId
     * @param {*} query 查询条件
     */
  async findcarIdByQuery(query) {
    const model = await this.deviceCarInfoDAO();
    const ret = await model.findAll({
      where: query,
      order: [['carId', 'DESC']]
    });
    return ret;
  }

  /**
   * 解除车辆绑定的电压方案,删除Redis缓存
   * @param {*} carId 
   * @param {*} id 
   */
  async delProgram(carId) {
    let model = await this.deviceCarInfoDAO();
    let res = await model.update({
      voltageProgramId: null
    }, {
      where: {
        carId
      }
    });
    // 删除Redis中的缓存
    await xcRedis.del(`${imeiVoltageProgram}${carId}`);
    return res;
  }

  /**
   * 根据imei从数据库中模糊查询获取绑定信息
   * @param {*} imei
   * @param {*} agentId
   */
  async carIdlikeBindimeiDAO(imei) {
    let bindInfoDAO = await this.deviceBindingInfoDAO();
    let ret = await bindInfoDAO.findOne({
      where: {
        imei: {
          $like: `%${imei}`
        }
      },
      raw: true,
      attributes: ['imei', 'carId']
    });
    if (!ret || !ret.carId) { 
      return {};
    }
    return ret;
  }

  static async getInvestorByCarIds(carIds) {
    let sql = `select DISTINCT(investor) from xc_ebike_2_carinfo where carId in (${carIds})`;
    const ret = await xcSequelize.query(sql, { type: Sequelize.QueryTypes.SELECT });
    return ret;
  }
}

module.exports = XCEBikeEBike;
