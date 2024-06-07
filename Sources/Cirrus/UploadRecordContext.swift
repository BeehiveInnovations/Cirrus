@_implementationOnly import CKRecordCoder
import CloudKit
import CloudKitCodable
import Foundation
import os.log

final class UploadRecordContext<Persistable: CloudKitCodable>: RecordModifyingContext {

  private let defaults: UserDefaults
  private let zoneID: CKRecordZone.ID
  private let logHandler: (String, OSLogType) -> Void

  private lazy var uploadBufferKey = "UPLOADBUFFER-\(zoneID.zoneName))"

  var recordIDsToDelete: [CKRecord.ID] = []

  init(
    defaults: UserDefaults, zoneID: CKRecordZone.ID,
    logHandler: @escaping (String, OSLogType) -> Void
  ) {
    self.defaults = defaults
    self.zoneID = zoneID
    self.logHandler = logHandler
  }

  func buffer(_ values: [Persistable]) {
    let records: [CKRecord]
    do {
      records = try values.map { try encodedRecord($0) }
    } catch let error {
      logHandler("Failed to encode records for upload: \(String(describing: error))", .error)
      records = values.compactMap { try? encodedRecord($0) }
    }
    
    // optimization
    updateRecordsToAdd(records)
  }
  
  func removeFromBuffer(_ values: [Persistable]) {
    // Map Persistable items to CKRecords and collect their IDs
    let recordIDsToRemove = values.compactMap { try? encodedRecord($0) }.map { $0.recordID }
    
    updateRecordsIDToRemove(recordIDsToRemove)
  }

  // MARK: - RecordModifying

  let name = "upload"

  var recordsToSave: [CKRecord.ID: CKRecord] {
    get {
      guard let data = defaults.data(forKey: uploadBufferKey) else { return [:] }
      do {
        return try NSKeyedUnarchiver.unarchiveTopLevelObjectWithData(data) as? [CKRecord.ID: CKRecord] ?? [:]
      } catch {
        logHandler("Failed to decode CKRecord.IDs from defaults key uploadBufferKey", .error)
        return [:]
      }
    }
    set {
      let prevRecords = self.recordsToSave
      
      do {
        if prevRecords.count != newValue.count {
          logHandler("Updating \(self.name) buffer with \(newValue.count) items", .info)
        }
        
        let data = try NSKeyedArchiver.archivedData(withRootObject: newValue, requiringSecureCoding: true)
        
        defaults.set(data, forKey: uploadBufferKey)
      } catch {
        logHandler("Failed to encode record ids for upload: \(String(describing: error))", .error)
      }
    }
  }

  func encodedRecord<T: CloudKitCodable>(_ obj: T) throws -> CKRecord {
    try CKRecordEncoder(zoneID: zoneID).encode(obj)
  }
  
  func modelChangeForUpdatedRecords<T: CloudKitCodable>(recordsSaved: [CKRecord], recordIDsDeleted: [CKRecord.ID]) -> SyncEngine<T>.ModelChanges {
    let models: Set<T> = Set(
      recordsSaved.compactMap { record in
        do {
          let decoder = CKRecordDecoder()
          return try decoder.decode(T.self, from: record)
        } catch {
          logHandler("Error decoding item from record: \(String(describing: error))", .error)
          return nil
        }
      })

    updateRecordsToRemove(recordsSaved)

    return .init(updates: .updatesPushed(models))
  }

  func failedToUpdateRecords<T: CloudKitCodable>(recordsSaved: [CKRecord], recordIDsDeleted: [CKRecord.ID]) -> SyncEngine<T>.ModelChanges {
    let models: Set<T> = Set(
      recordsSaved.compactMap { record in
        do {
          let decoder = CKRecordDecoder()
          return try decoder.decode(T.self, from: record)
        } catch {
          logHandler("Error decoding item from record: \(String(describing: error))", .error)
          return nil
        }
      })
    
    updateRecordsToRemove(recordsSaved)
    
    return .init(unknownUpdates: .unknownItemsPushed(models))
  }
  
  /// Optimized save to `recordsToSave`
  /// - Parameter recordsSaved: records to remove
  private func updateRecordsToRemove(_ removeRecords: [CKRecord]) {
    updateRecordsIDToRemove(removeRecords.map { $0.recordID })
  }
  
  private func updateRecordsIDToRemove(_ removeRecordIDs: [CKRecord.ID]) {
    // Access recordsToSave once, modify, and then update if necessary
    var currentRecordsToSave = recordsToSave
    
    removeRecordIDs.forEach { currentRecordsToSave.removeValue(forKey: $0) }
    
    // Assuming recordsToSave has a setter
    recordsToSave = currentRecordsToSave
  }
  
  /// Optimized save to `recordsToSave`
  /// - Parameter recordsSaved: records to remove
  private func updateRecordsToAdd(_ addRecords: [CKRecord]) {
    // Access recordsToSave once, modify, and then update if necessary
    var currentRecordsToSave = recordsToSave
    
    addRecords.forEach { currentRecordsToSave[$0.recordID] = $0 }
    
    // Assuming recordsToSave has a setter
    recordsToSave = currentRecordsToSave
  }
}
