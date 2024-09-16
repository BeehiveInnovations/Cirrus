import CloudKit
import Foundation
import os.log

public enum AccountStatus: Equatable {
  case unknown
  case couldNotDetermine
  case available
  case restricted
  case noAccount
}

extension SyncEngine {
  
  // MARK: - Internal
      
  internal func observeAccountStatus() async throws -> AccountStatus {
    logHandler(#function, .debug)
    
    guard let container else {
      self.accountStatus = .noAccount
      
      return .noAccount
    }
    
    let status = try await container.accountStatus()
    
    switch status {
      case .available:
        accountStatus = .available
      case .couldNotDetermine:
        accountStatus = .couldNotDetermine
      case .noAccount:
        accountStatus = .noAccount
      case .restricted:
        accountStatus = .restricted
      default:
        accountStatus = .unknown
    }
    
    return accountStatus
  }
}
